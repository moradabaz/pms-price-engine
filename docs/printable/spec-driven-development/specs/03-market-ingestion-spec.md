# Phase 3 — Market Ingestion (Kinesis)

**Status:** Draft
**Depends on:** Phase 0 (repo setup — `market-price-events` Kinesis stream already provisioned by `infra/localstack/init-aws.sh`, 4 shards)
**Blocks:** Phase 4 (Flink processing) consumes `market-price-events` produced here
**Related:** [ADR-0001](../../../docs/adr/ADR-0001-kafka-kinesis-split.md), [ADR-0005](../../../docs/adr/ADR-0005-market-price-partition-key.md), [`market_price.v1.json`](../../events/market_price.v1.json), [Phase 2 spec](../02-cdc-pipeline/spec.md) (sibling ingestion phase, different transport)

---

## 1. Executive summary (plain language)

Phase 2 gave the pricing engine one half of the picture: what an apartment actually costs to run. This phase gives it the other half: **what similar apartments are charging right now**, for a given city, neighborhood, and property type. Without this, "minimum viable price" (cost + margin) is the only number the engine could ever produce — never "competitive price," which is the whole point of a dynamic pricing tool.

Unlike Phase 2, there is no existing system to observe here — no database, no WAL, nothing to do CDC against. A small standalone service (`market-ingestor`) **originates** this data itself: a mock market-data generator that publishes believable competitive-pricing snapshots directly to Kinesis, on an interval, indefinitely — architecturally the market-side equivalent of Phase 1's `mock-pm-app`, except it publishes straight to a stream instead of writing to a table Debezium later reads.

By the end of this phase we can prove: *"a market-price snapshot is generated for a given city/neighborhood/property-profile/date, published to `market-price-events`, and is retrievable from Kinesis, schema-valid, with all snapshots of the same market segment landing on the same shard."* This phase does **not** touch Flink, does not join anything against `payment-events.v1`, and does not compute a price.

---

## 2. Scope

### In scope

- `services/market-ingestor` (already has a `pyproject.toml` stub — this phase fills in `src/`): a long-running Python process that generates synthetic `market_price.v1` snapshots and publishes them to the `market-price-events` Kinesis stream via `boto3`.
- A hand-written `MarketPrice` Pydantic model in `libs/shared-schemas` (its first real consumer — the package has existed as an empty shell since Phase 0), used to validate every event **before** publishing, not after.
- Kinesis partition-key construction per [ADR-0005](../../../docs/adr/ADR-0005-market-price-partition-key.md): a plain string built from the market segment, no application-level hashing.
- Manual verification against the running LocalStack stack (AWS CLI `get-shard-iterator` / `get-records`), the Kinesis equivalent of Phase 2's `kcat`-based verification.

### Out of scope (explicitly deferred)

- Real market data — `scraped_airbnb`, `scraped_booking`, or `inside_airbnb` `data_source` values. This phase always publishes `data_source: "mock"`. Pulling the real Inside Airbnb public dataset is a candidate for a later hardening pass, not this phase (see §7, Known limitations).
- Creating the Kinesis stream itself — already done in `infra/localstack/init-aws.sh` (Phase 0).
- Any Flink consumption, join against `payment-events.v1`, or pricing computation (Phase 4).
- Automated integration test harness that spins up the full stack — matches Phase 2's precedent (§2 of that spec); verification here is manual, same philosophy.
- A seed/backfill step. Unlike Phase 1 (`payment_lines` is a queryable Postgres table Debezium can snapshot on startup), Kinesis is a transient transport with a default 24h retention window — there is no equivalent of "backfill existing rows." Whatever `market-ingestor` publishes after it starts is the only history that will ever exist on the stream itself (see §7).

---

## 3. Data contract

The event contract is [`specs/events/market_price.v1.json`](../../events/market_price.v1.json) — already fully specified, no changes needed here. One property is load-bearing for this phase's producer logic specifically, and is easy to get wrong by analogy with Phase 2:

**`event_id` here is a pure idempotency/replay-dedup key, not a stable row identity.** This is the opposite mutability model from `payment_line.v1` (ADR-0003): a `payment_line` event's `event_id` is reused across an `INSERT` and all subsequent `UPDATE`s of the same row, and consumers upsert-by-`event_id`. A `market_price` event is a **point-in-time snapshot** — every publish is a brand-new observation with a fresh `event_id` (`uuid4()`), never a mutation of a previous one. Consumers (Phase 4's Flink job) determine "the current market price for this segment/date" by comparing `collected_at` across multiple distinct events for the same `(market_area, property_profile, target_date)`, not by tracking one `event_id` over time. This distinction must be documented in the producer's code, not just here — the two contracts look superficially similar (both have an `event_id` field) but mean different things.

---

## 4. Market-ingestor behavior

- **Market segments.** A fixed, config-driven set of `(city, neighborhood, property_profile)` combinations — the producer does not invent arbitrary segments per tick, it cycles through a known list (§5.2), so the same segment reliably recurs across ticks (needed for AC-04's partition-affinity check, and for Flink to eventually build up a time series per segment).
- **Target dates.** ~~Each tick, for every configured segment, the generator publishes **one** snapshot for a `target_date` chosen at random within a rolling forecast window (`MARKET_INGESTOR_FORECAST_DAYS`, §5.2) ahead of today.~~ **Superseded 2026-07-29 (Phase 4, Decision D.1 — [`docs/phase-4-streaming-design-decisions.md`](../../../docs/phase-4-streaming-design-decisions.md)):** the random pick left Hoja 2 of Flink's join (Decision D) permanently partial, with no explainable pattern to which nights had a price. Replaced with a **deterministic cyclic offset shared by every segment in a tick**: `offset_days = 1 + (tick_count % forecast_days)`, `target_date = today + offset_days`, computed fresh at publish time (never cached), so the window slides on its own as real days pass — no special-cased logic needed. With the defaults (`tick_interval=60s`, `forecast_days=60`) one full cycle takes `60 × 60s = 1h`: every segment covers all 60 nights once per hour, and every night is refreshed at least once per hour in steady state — this is also what makes `collected_at`'s freshness meaning concrete (§3): under this scheme no market data is ever more than ~1h stale absent an incident. Still keeps volume flat and bounded per tick (segments × 1, not segments × window).
- **Pricing values are derived from a synthetic sample, not generated independently.** `avg_nightly_rate`/`p25`/`p50`/`p75` are **not** four separately randomized numbers — that risks statistically incoherent output (e.g. `p75 < p25`) with no code path preventing it. Instead, for each segment/tick, the generator draws `sample_size` synthetic per-listing nightly prices from a **log-normal distribution** (the standard shape for rental-price markets — right-skewed, matching how real listings actually distribute) parameterized by the segment's reference median price (§5.2), then computes `avg_nightly_rate`/`p25`/`p50`/`p75` directly from that drawn sample. This guarantees internally consistent ordering by construction, and anchors the output to real 2025–2026 market observations (§5.2's sources) instead of arbitrary ranges.
- **Deliberately not a full Inside Airbnb listing replica.** The synthetic per-listing draws exist only to produce a coherent price sample — they are not persisted, exposed, or modeled with Inside Airbnb's full raw-listing schema (`host_id`, `license`, `minimum_nights`, `reviews_per_month`, lat/long, etc.). None of those fields feed `market_price.v1` (§3) — building a full listing model to populate columns nothing downstream reads would be scope creep with no consumer, not realism.
- **Low-confidence samples.** `sample_size` is a random integer in `[3, 45]`, deliberately allowed to fall below 10 (the schema's own "flag as low-confidence" threshold, `specs/events/market_price.v1.json` description) — so Phase 4 has real low-confidence data to eventually handle, not just idealized high-sample snapshots.
- **`occupancy_rate`** is an independent random value in `[0.45, 0.85]` — a plausible band, not derived from per-neighborhood data (no granular occupancy-by-neighborhood source was found during research; documented as an approximation, not a researched figure, unlike the price table in §5.2).
- **Provenance.** `market_context.data_source` is always `"mock"`, `platform` is always `null` — matches Phase 1's `source: 'synthetic'` precedent of making synthetic data always distinguishable from anything real.
- **Publishing.** Each tick's batch of events (one per segment) is sent via a single `put_records` (batch API, up to 500 records/call) rather than one `put_record` call per event — the natural production pattern for Kinesis, and free to adopt here since one tick's volume already fits comfortably in one batch call.
- **Failure handling on publish.** Kinesis's `put_records` does not raise an exception for a partial batch failure — the call succeeds at the API level, but its response carries a `FailedRecordCount` and marks each failed record with an `ErrorCode` (e.g. `ProvisionedThroughputExceededException`). After every `put_records` call, the response is inspected; any failed records are resent **unmodified** — same serialized bytes, same `event_id`, same `collected_at`, never regenerated (a retry that regenerates the event isn't retrying delivery, it's publishing a different snapshot by accident) — up to `MARKET_INGESTOR_PUBLISH_MAX_RETRIES` attempts with exponential backoff (`MARKET_INGESTOR_PUBLISH_BACKOFF_BASE_SECONDS`, §5.3). If retries are exhausted, the remaining failed records are logged at `WARNING` (segment + `ErrorCode`, via `libs/common`'s `structlog`) and dropped — the tick continues rather than blocking or crashing the whole service over one unhealthy shard.

  This is a deliberate design choice, not an oversight, and worth being explicit about in terms of Kleppmann's delivery-semantics framing (*Designing Data-Intensive Applications*, ch. 11, Stream Processing): retrying is an **at-least-once** delivery strategy — it can produce a genuine duplicate when a *whole-call* failure is ambiguous (a dropped connection means the client cannot tell whether Kinesis actually received the batch before the connection failed, only a partial per-record failure is unambiguous). Kleppmann's own point is that **true exactly-once delivery is not achievable at this layer** (equivalent to the Two Generals' Problem — a sender can never be fully certain an acknowledgment loss means the message was lost too). What's actually achievable, and what this design relies on, is *effectively-once processing*: at-least-once delivery (retry) plus idempotent consumption. That consumption side is already guaranteed by §3 — Phase 4 must compare `collected_at` across multiple events for the same `(market_area, property_profile, target_date)` rather than trust any single `event_id` as a stable identity, so a duplicate delivery is harmless by construction, the same pattern ADR-0003 established for `payment_line`. The residual `at-most-once` fallback (log-and-drop only after retries are exhausted) is accepted solely as the terminal case for a persistently unhealthy shard — an expected rare edge, not the normal path.
- **Partition key.** Per ADR-0005: `f"{city}|{neighborhood or ''}|{property_type}|{bedrooms}"`, passed as-is as each record's `PartitionKey` — Kinesis hashes it internally, the producer does not. **This must be a code comment at the point the key is built, not just a fact living in an ADR** — a future reader of `market-ingestor`'s source should not need to go spelunking through `docs/adr/` to understand why the code looks the way it does, or to learn that any resulting shard-load skew is expected, not a bug.

---

## 5. Configuration

Four things need concrete values before implementation has no open decisions left: which segments exist, how their synthetic prices are generated, the publish cadence, and the Kinesis client wiring.

### 5.1 Market segments (fixed list, not generated)

| City | Neighborhoods | Property profiles |
|---|---|---|
| Barcelona | Eixample, Gràcia | studio (0BR), apartment (1BR), apartment (2BR) |
| Madrid | Centro, Chamberí | studio (0BR), apartment (1BR), apartment (2BR) |
| Valencia | Ruzafa, El Carmen | studio (0BR), apartment (1BR), apartment (2BR) |

3 cities × 2 neighborhoods × 3 profiles = **18 distinct segments**, spread over the stream's 4 shards (per ADR-0005, several segments will legitimately share a shard — by design, not a bug to fix). Small enough to keep this a PoC, large enough that ADR-0005's hot-shard discussion is actually observable rather than purely theoretical (Barcelona alone contributes 6 of the 18 segments — worth watching whether its shard concentrates more traffic than the others, per ADR-0005's own prediction).

### 5.2 Reference prices & synthetic sampling

Reference median nightly rate (EUR, `property_profile` = 1-bedroom apartment) per segment, anchored to 2025–2026 market observations from secondary aggregate reports (not a raw Inside Airbnb CSV pull — treat as directional anchors, not exact statistics):

- [AirROI — Barcelona, Catalonia Airbnb Data 2026](https://www.airroi.com/airbnb-data/spain/catalonia/barcelona)
- [AirDNA — Barcelona Short-Term Rental Data (2026)](https://www.airdna.co/vacation-rental-data/app/es/barcelona/barcelona/overview)
- [Rate Ranger — Barcelona Hotel Prices: Best Neighbourhoods](https://www.rateranger.io/blog/barcelona-hotel-guide/)
- [AirROI — Madrid, Community of Madrid Airbnb Data 2025](https://www.airroi.com/report/world/spain/community-of-madrid/madrid)
- [Investropa — Is Airbnb still profitable for owners in Madrid?](https://investropa.com/blogs/news/madrid-airbnb-still-profitable-owners)
- [AirROI — Valencia, Valencian Community Airbnb Data 2026](https://www.airroi.com/airbnb-data/spain/valencian-community/valencia)
- [Inside Airbnb Project data documentation (UCLA)](https://airbnbproject.humspace.ucla.edu/the-data/) — raw listing column reference (`room_type`, `price`, `neighbourhood`, etc.), used to confirm which fields a real dataset carries, not as a price source.

| City | Neighborhood | Reference median (1BR, EUR/night) | Basis |
|---|---|---|---|
| Barcelona | Eixample | 160 | High end of Eixample's researched €180–312 in-season range, discounted for off-season/1BR (vs. entire-home average). |
| Barcelona | Gràcia | 110 | ~30% below Eixample, per researched "Gràcia runs 20–40% less than Eixample-equivalent." |
| Madrid | Centro | 130 | Within Madrid's researched top-tier band (prime locations €175+), scaled down for 1BR vs. premium entire-home. |
| Madrid | Chamberí | 95 | Upscale-residential, closer to Madrid's overall researched ADR (€115–150) than to Centro's prime pricing. |
| Valencia | Ruzafa | 140 | Above Valencia's researched overall ADR (€136–153) — a trendy, in-demand neighborhood. |
| Valencia | El Carmen | 125 | Near Valencia's researched overall ADR — historic center, consistently popular but not a premium outlier. |

Property-profile multiplier, applied to the segment's reference median above:

| `property_profile.type` | `bedrooms` | Multiplier |
|---|---|---|
| `studio` | 0 | `0.7` |
| `apartment` | 1 | `1.0` (reference) |
| `apartment` | 2 | `1.45` |

**Sampling procedure** (executed once per segment, per tick):

1. `segment_median = REFERENCE_PRICES[city, neighborhood] * PROFILE_MULTIPLIER[type, bedrooms]`
2. **Added 2026-07-29 (Phase 4, Decision D.2):** `seasoned_median = segment_median * SEASONAL_MULTIPLIER[target_date.month]` — a fixed 3-tier table by month, same for all 3 cities in this first version (`services/market-ingestor/src/market_ingestor/seasonality.py`): verano (Jul–Aug) `×1.30`, hombro (May/Jun/Sep/Oct) `×1.05`, invierno (Nov–Apr) `×0.85`. Applied before the log-normal parameterization below, so Phase 4 never has to know seasonality exists — it always reads an already-seasoned `avg_nightly_rate_eur(target_date)`.
3. Target coefficient of variation `CV = 0.35` (a realistic spread for a rental-price market — not itself sourced from a specific report, chosen as a plausible middle ground). Log-normal parameters: `sigma = sqrt(ln(1 + CV**2))` (≈ `0.343` for `CV = 0.35`), `mu = ln(seasoned_median) - sigma**2 / 2` (so the distribution's **median**, not its mean, equals `seasoned_median` — a log-normal's mean is always above its median, which is the realistic direction for a right-skewed price distribution).
4. Draw `sample_size` values from `lognormal(mu, sigma)` — these are the synthetic per-listing nightly prices for this segment/tick. Not persisted or exposed individually (see §4).
5. `avg_nightly_rate = mean(draws)`, `p25/p50/p75 = percentile(draws, [25, 50, 75])` — computed directly from the drawn sample, never set independently.

### 5.3 Market-ingestor service (`services/market-ingestor`)

Settings via `pydantic-settings` (prefix `MARKET_INGESTOR_`), same pattern as `mock-pm-app`'s `MOCK_APP_*` (Phase 1 precedent).

| Env var | Default | Why |
|---|---|---|
| `MARKET_INGESTOR_KINESIS_ENDPOINT_URL` | *(required, no default)* | Same reasoning as Phase 1's `MOCK_APP_POSTGRES_DSN` (§5.2 of that spec): no safe default exists across bare-metal, Compose, and real-AWS contexts, so it must be set explicitly rather than silently defaulting to a LocalStack URL that would be wrong in the real-AWS demo. Set to `http://localstack:4566` in `infra/docker-compose.yml`'s environment block (below); left **unset** in the real-AWS demo environment, which makes boto3 fall back to its default AWS endpoint resolution — per ADR-0001's consequences section. |
| `MARKET_INGESTOR_KINESIS_STREAM_NAME` | `market-price-events` | Matches the stream already created in `infra/localstack/init-aws.sh`. |
| `MARKET_INGESTOR_AWS_REGION` | `eu-west-1` | Matches `infra/localstack/init-aws.sh`'s `REGION`. |
| `MARKET_INGESTOR_TICK_INTERVAL_SECONDS` | `60` | One publish cycle (18 events, one batch `put_records` call) per minute — market prices don't need `mock-pm-app`'s 10–30s cadence to feel "live" for demo purposes; a single fixed interval (not a randomized range) is enough here, keeping the generator simpler than Phase 1's. |
| `MARKET_INGESTOR_FORECAST_DAYS` | `60` | Rolling window of future `target_date`s to sample from per tick, per segment (§4). |
| `MARKET_INGESTOR_PUBLISH_MAX_RETRIES` | `3` | Bounded retry count for `put_records` failed-record resends (§4, Failure handling) — unbounded retry would let a persistently unhealthy shard stall the tick loop indefinitely. |
| `MARKET_INGESTOR_PUBLISH_BACKOFF_BASE_SECONDS` | `0.5` | Exponential backoff base between retry attempts (`base * 2^attempt`) — e.g. `0.5s, 1s, 2s` across 3 retries. |
| `MARKET_INGESTOR_LOG_LEVEL` | `INFO` | Via `libs/common`'s existing `structlog` setup. |

Docker Compose addition (new service in `infra/docker-compose.yml`, alongside `mock-pm-app`):

```yaml
market-ingestor:
  build:
    context: ..
    dockerfile: services/market-ingestor/Dockerfile
  container_name: pms_market_ingestor
  depends_on:
    localstack:
      condition: service_healthy
  environment:
    MARKET_INGESTOR_KINESIS_ENDPOINT_URL: http://localstack:4566
    AWS_ACCESS_KEY_ID: test
    AWS_SECRET_ACCESS_KEY: test
  restart: unless-stopped
```

Build context is the repo root, same reasoning as `mock-pm-app` (Phase 1 §5.2) — this service is a uv workspace member needing sibling packages (`libs/shared-schemas`, `libs/common`) and the root lockfile.

---

## 6. Acceptance criteria

Verification method: AWS CLI against the running LocalStack stack (`aws --endpoint-url=http://localhost:4566 kinesis ...`), the Kinesis equivalent of Phase 2's `kcat`/`psql` manual verification — no automated integration harness (§2).

- **AC-01 — Producer publishes cleanly, retrying transient failures.** Given LocalStack is up and `market-price-events` exists, when `market-ingestor` calls `put_records`, any partially failed records are resent (unmodified) up to `MARKET_INGESTOR_PUBLISH_MAX_RETRIES` times with backoff (§4); under normal LocalStack operation, `FailedRecordCount` is 0 after retries complete for every tick. A record still failing after retries is logged with its segment and `ErrorCode` and dropped — not silently lost without a trace, and not a reason to crash the process.
- **AC-02 — Live publish cadence.** Within one tick interval after startup, exactly 18 new records (one per configured segment, §5.1) are retrievable across the stream's 4 shards combined — verified on the **data plane** (`get-shard-iterator` + `get-records` on every shard, summed), not by trusting the producer's own logs.
- **AC-03 — Schema conformance.** Every retrieved record's payload validates against `market_price.v1.json` via `specs/contracts/` (`uv run pytest specs/contracts/test_market_price_contract.py`), with zero additional properties — checked against live records pulled from the stream, not only the static fixtures.
- **AC-04 — Segment partition affinity.** Across at least 3 consecutive ticks, every record for a given fixed segment (e.g. Barcelona/Eixample/apartment/2BR) lands on the same shard ID — the Phase 3 equivalent of Phase 2's AC-05, and the concrete test of ADR-0005's core claim.
- **AC-05 — Snapshot immutability is safe to replay.** Restarting `market-ingestor` produces new events with fresh `event_id`s for the same segments — no crash, no attempt to resume or deduplicate against previously published state (correct, since §3 established these are point-in-time snapshots, not upserts). Verified by restarting the container and confirming AC-02 holds again immediately.
- **AC-06 — Synthetic data is distinguishable.** Every retrieved record has `market_context.data_source == "mock"` and `market_context.platform == null` — never a real-data enum value, mirroring Phase 1's AC-03 (`source = 'synthetic'`).

---

## 7. Known limitations

- **No seed/backfill.** Unlike `payment_lines`, there is no historical market data before `market-ingestor` starts publishing — Kinesis's default 24h retention means even what's published isn't permanently queryable from the stream itself. If Phase 5 (Iceberg) needs deep market history for dbt models, that requires Iceberg to persist Flink's consumed snapshots over time as its own historical store — not a Kinesis backfill, which isn't a concept Kinesis supports the way Postgres snapshots are.
- **No real market data.** Always `data_source: "mock"`. Pulling the real Inside Airbnb dataset (already a valid enum value in the schema) for seed-time realism is a deferred enhancement, not required to call this phase done.
- **No automated integration tests.** Matches Phase 2's precedent (its §2, Out of scope) — verification is manual per §6.
- **Hot-shard risk is real and intentional**, per [ADR-0005](../../../docs/adr/ADR-0005-market-price-partition-key.md) — if one shard (plausibly Barcelona's, given it contributes 6 of 18 segments) absorbs visibly more traffic than the others, that is the expected outcome of a deliberate learning decision, not a defect. Worth an `error-handling/` write-up **only if** it's actually observed and causes a concrete effect (e.g. a `ProvisionedThroughputExceededException`), per this project's practice of documenting real incidents, not hypothetical ones.

---

## 8. Follow-ups for later phases

- **Phase 4 has an unresolved, genuinely open dependency this phase surfaced: nothing in the project currently maps an `apartment_id` to a market segment.** `payment_lines.sql` (Phase 1) only has `apartment_id` and `apartment_reference` (a `<CITY>-<NNN>` string) — no city, neighborhood, property type, or bedroom count as real columns. Phase 4's Flink job cannot join `payment-events.v1` against `market-price-events` without first resolving, for every apartment, which market segment it belongs to. This is not this phase's problem to solve (Phase 3 only produces the market side), but it must be resolved before Phase 4 can be specified, let alone implemented. Candidate approaches to evaluate then, not now: (a) add real columns to `payment_lines` (a migration against an already-merged Phase 1 table), (b) a small, separately-owned apartment-reference dataset/table Flink reads directly, (c) parsing `apartment_reference`'s `<CITY>-<NNN>` pattern for the city only (fragile, and gives no neighborhood or property-profile signal at all). Flagging this now, explicitly, is exactly the kind of thing this project's spec-driven approach is supposed to catch before it becomes a Phase 4 blocker discovered mid-implementation.
- **Phase 4 must implement its own idempotency/freshness logic for `market_price` consumption** — keyed by `(market_area, property_profile, target_date)`, picking the event with the latest `collected_at`, per §3. This is a different consumption pattern from `payment_line`'s upsert-by-`event_id` (ADR-0003) and must not be implemented the same way by copy-paste.
- **Phase 5 may want to persist raw market snapshots directly** (not just Flink's joined/derived output) if historical market-trend analysis ever becomes a dbt model target — an open question, not decided here.
