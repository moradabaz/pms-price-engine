# Phase 26 — Market Pulse Job (windowed streaming metrics)

**Status:** Draft
**Depends on:** Phase 3 (market ingestion — `market-price-bridge.v1`), Phase 4 (Flink processing —
`booking-events.v1`, the `price_decision` DynamoDB table and its key shape), Phase 18 (owner
contract / cost fields on `PriceDecision`)
**Blocks:** nothing — this is a read-only, additive analytics job. No other phase depends on it.
**Does not touch:** `streaming/flink-jobs/` (the existing pricing engine job) — this phase adds a
**second, independent** Flink job, deployed and run separately, per an explicit decision made
before this spec was written.
**Related:** [ADR-0008](../../../docs/adr/ADR-0008-kinesis-kafka-bridge.md) (precedent for a
"bridge is the wrong tool, look for a native connector or a lookup instead" decision),
[`docs/technical-definitions.md`](../../../docs/technical-definitions.md),
[`docs/profitable-pricing-glossary.md`](../../../docs/profitable-pricing-glossary.md)

---

## 1. Executive summary (plain language)

Every metric this project has computed so far is triggered by a single event, and answers "what is
true right now." This phase asks a different kind of question: "how many bookings came in over the
last 5 minutes, what is the market doing right now, and how profitable were those bookings, on
average, after every booking and owner cost?" That is a **windowed** question — it needs to
observe a slice of time, not a single event — and this project has no windowed computation
anywhere yet.

This phase is explicitly **pedagogical**: its purpose is to introduce Flink's event-time windowing
API (`TumblingEventTimeWindows`, watermarks, allowed lateness) and Asynchronous I/O
(`AsyncDataStream`), two capabilities the existing pricing job never needed because it is pure
per-event `KeyedCoProcessFunction` state, with zero time windows (confirmed by grep across
`streaming/flink-jobs/src/flink_jobs/`, Phase 4 onward: not one `Window` call anywhere). It is
explicitly **not** a replacement or an extension of the pricing engine, and produces a different
kind of output: rolled-up, time-boxed observability metrics for a property manager's dashboard, not
a pricing decision.

---

## 2. Architecture

```
market-price-bridge.v1 (Kafka)  ──────────────┐
                                               ▼
                                    MarketPulseWindowFunction
                                    keyBy(market_area)
                                    TumblingEventTimeWindows(5 min)
                                    on collected_at
                                               │
booking-events.v1 (Kafka)  ──────► BookingEnrichmentFunction (Async I/O)
                                    GetItem(price_decision, apartment_id + check_in)
                                               │
                                               ▼
                                    BookingPulseWindowFunction
                                    keyBy(apartment_id)
                                    TumblingEventTimeWindows(5 min)
                                    on created_at
                                               │
                          ┌────────────────────┴───────────────────┐
                          ▼                                        ▼
                 DynamoDB: market_pulse_5min           DynamoDB: booking_pulse_5min
```

Deployed as a new, independent package, `streaming/market-pulse-job/`, mirroring
`streaming/flink-jobs/`'s own shape (own `pyproject.toml`, own `main.py`, its own Flink job
submission). It is a second job running on the same Flink cluster, not a new operator added to the
existing pricing job's graph. This means:

- The existing job's checkpointing, parallelism, and failure domain are completely unaffected by
  this one crashing, backpressuring, or being redeployed.
- It reads `booking-events.v1` and `market-price-bridge.v1` as an **independent consumer group** —
  never affects the existing job's consumer offsets or throughput.
- It talks to DynamoDB only via `GetItem` (read) and `PutItem` (write to its own two new tables,
  §5) — it never writes to the `price_decision` table the pricing job owns.

### 2.1 Why Async I/O instead of a second CDC bridge

The obvious analogue to how the pricing job resolves owner contracts/costs would be a third
`KeyedCoProcessFunction` input fed by a DynamoDB-Streams-to-Kafka bridge (the same shape as
[ADR-0008](../../../docs/adr/ADR-0008-kinesis-kafka-bridge.md)'s Kinesis-to-Kafka bridge for
`price_decision` change events). That was considered and rejected here:

- It would require standing up a **third bridge service** and a **third topic**, purely to look up
  a value that already has a stable, well-known key (`apartment_id` + `target_date`) in an
  already-queryable store.
- The pricing job's own DynamoDB table is already the system of record for "the latest decision
  for this apartment/date" — a direct point lookup is the natural fit, not a second streaming join.
- It is a genuinely different, and currently unused, Flink capability
  (`AsyncDataStream.unordered_wait`) worth exercising for its own pedagogical sake, per this
  phase's stated purpose (§1) — a second `KeyedCoProcessFunction` would only repeat a pattern the
  project already has three of (Phase 4's Stage A/A2/A4).

Trade-off accepted: an async DynamoDB `GetItem` per booking event adds real per-record I/O latency
and a dependency on DynamoDB's availability for this job's enrichment step (mitigated by
`AsyncDataStream`'s built-in timeout/retry and a defined fallback, §4.3) — a CDC-fed keyed-state
join would have zero per-event I/O once state is warm, at the cost of the extra bridge/topic. For a
5-minute-windowed analytics metric (not the priced-decision floor itself), the operational
simplicity of one fewer service was judged to matter more than shaving lookup latency.

---

## 3. Windowing design

**Event time, not processing time**, on all three windows — deliberately, since this is the
pedagogical point of the phase (§1):

- Market pulse: watermark and window key on `MarketPrice.collected_at`.
- Booking count / profit: watermark and window key on `Booking.created_at`.
- Watermark strategy: `WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))` on
  both streams — a fixed, explicit out-of-orderness bound rather than a monotonic watermark,
  because Kafka delivery order across partitions gives no actual ordering guarantee across
  different apartments/segments.
- Window: `TumblingEventTimeWindows.of(Time.minutes(5))`, non-overlapping, one output row per
  key per window.
- **Allowed lateness:** `allowedLateness(Time.minutes(1))`. A record arriving after its window has
  closed, but within this grace period, updates and re-emits that window's result (Flink's
  standard late-firing behavior). A record arriving later than that is sent to a
  `late-data` side output — logged, never silently dropped, matching the project's existing
  "explicit, not silent" convention for exceptional cases (`errors.tolerance: none` in Debezium,
  `manual_overrides`' auditability, etc.).

---

## 4. Metric definitions

### 4.1 Market pulse (average market price per segment, per 5 min)

- **Input:** `market-price-bridge.v1`, filtered to blended snapshots only (`platform` field is
  `None` — channel-specific snapshots from Phase 16 are excluded, matching how the existing
  pricing job's top-level calculation only ever reads the blended rate too).
- **Key:** `market_area` (the same `"{city}/{neighborhood}"` string the existing pricing job
  already builds in `stage_price_decision.py`).
- **Aggregate:** count, average, min, max of `avg_nightly_rate_eur` within the window.

### 4.2 Booking count (per apartment, per 5 min)

- **Input:** `booking-events.v1`, filtered to `status == "confirmed"`.
- **Key:** `apartment_id`.
- **Aggregate:** count of bookings whose `created_at` falls in the window. (Portfolio-wide totals,
  if wanted on the dashboard, are a `SUM` over apartments at query time — not computed here, since
  a finer granularity is always aggregable upward and the reverse is not.)

### 4.3 Average profit per booking (per apartment, per 5 min)

For each confirmed booking, an async lookup resolves the most recently known cost/commission
context for that `(apartment_id, target_date=check_in)` from the `price_decision` DynamoDB table
(the same table and key shape `dynamodb_sink.py` already writes, §5.1), then computes:

```
nights = (check_out - check_in).days
total_cost_eur = fixed_and_allocated_costs_eur * nights + per_booking_cost_eur
profit_eur = revenue_eur * (1 - p) - total_cost_eur
```

where `fixed_and_allocated_costs_eur`, `per_booking_cost_eur`, and `p` are read directly from that
`price_decision`'s `cost_inputs` (no formula is duplicated — this evaluates
`libs/pricing-formulas`' own break-even numerator/`p` terms against the booking's *actual* revenue,
instead of solving for a price).

**Confirmed approximation (accepted per the design discussion preceding this spec):** this uses
the aggregate percentage rate `p` uniformly against total revenue. It does **not** apply each
individual percentage cost's own `revenue_base` netting (the mechanism described in
`docs/technical-definitions.md` / `docs/profitable-pricing-glossary.md` §14.4/§14.8) — that would
require carrying the full per-concept cost breakdown (not just the `p`/`commission_pct`/
`commission_base` aggregate fields) through to this job. Accepted explicitly as a reasonable
approximation for a rolled-up dashboard metric; the pricing engine's own floor calculation (Phase
4/11/20) remains exact and is entirely unaffected, since this job never writes back to
`price_decision`.

**Fallback when no `price_decision` exists yet for that `(apartment_id, target_date)`:** the async
lookup returns nothing (e.g. a booking for a brand-new apartment, or a date beyond the pricing
job's known horizon). That booking is still counted in §4.2's booking count, but excluded from the
profit average for its window — logged at `WARNING`, never silently treated as zero profit (a
missing cost basis and a genuinely zero-profit booking are not the same thing, and conflating them
would corrupt the average).

- **Key:** `apartment_id`.
- **Aggregate:** average of `profit_eur` over bookings with a resolved cost basis, within the
  window; also emits the count of bookings excluded for lack of a cost basis.

---

## 5. Data model

### 5.1 Reads (no new tables)

- `market-price-bridge.v1` (Kafka) — existing topic, `MarketPrice` schema, no changes.
- `booking-events.v1` (Kafka) — existing topic, `Booking` schema, no changes.
- `price_decision` (DynamoDB) — existing table, read-only `GetItem` by
  `{apartment_id, target_date}` — the exact key `dynamodb_sink.py` already writes with. No schema
  change; this job only reads `cost_inputs.fixed_and_allocated_costs_eur`,
  `cost_inputs.per_booking_cost_eur`, and `cost_inputs.p`.

### 5.2 Writes (two new DynamoDB tables, this phase's only new persisted state)

**`market_pulse_5min`**

| Field | Type | Notes |
|---|---|---|
| `market_area` (PK) | S | e.g. `"Barcelona/Eixample"` |
| `window_start` (SK) | S (ISO datetime) | window lower bound, inclusive |
| `window_end` | S (ISO datetime) | window upper bound, exclusive |
| `avg_price_eur` | N | |
| `min_price_eur` | N | |
| `max_price_eur` | N | |
| `sample_count` | N | number of snapshots aggregated |

**`booking_pulse_5min`**

| Field | Type | Notes |
|---|---|---|
| `apartment_id` (PK) | S | |
| `window_start` (SK) | S (ISO datetime) | |
| `window_end` | S | |
| `booking_count` | N | all confirmed bookings in the window |
| `avg_profit_eur` | N, nullable | null if every booking in the window lacked a cost basis |
| `profit_sample_count` | N | bookings actually included in `avg_profit_eur` |
| `unresolved_cost_count` | N | bookings excluded for lack of a `price_decision` |

Both tables are sinked the same way the existing job writes `price_decision` (`put_item`, no
upsert-merge semantics needed — each `(key, window_start)` is written exactly once per late-firing
update, DynamoDB `put_item` naturally overwrites on re-fire within the allowed-lateness period).

---

## 6. Scope

### In scope

- `streaming/market-pulse-job/` — a new, independently deployable PyFlink job package.
- The three windowed metrics (§4), each keyed and windowed as described.
- Async I/O enrichment of bookings against the existing `price_decision` DynamoDB table.
- The two new DynamoDB tables (§5.2) and their Terraform/LocalStack provisioning
  (`infra/localstack/init-aws.sh`, `infra/terraform/`).
- A late-data side output, logged, per window stream (§3).

### Out of scope (explicitly deferred)

- Any dashboard panel consuming these tables — a separate, later addition to `dashboard/`.
- Exact (non-approximated) profit accounting for every percentage cost's individual revenue base
  netting (§4.3) — would require widening `price_decision`'s persisted cost breakdown; deferred as
  a follow-up if the approximation proves misleading in practice.
- Portfolio-wide (all-apartments) rollups — left to query time (§4.2).
- Backfilling historical windows from data older than this job's first run — like the existing
  pricing job, this only computes going forward from deployment.
- Any change whatsoever to `streaming/flink-jobs/` (the existing pricing job), its topics, its
  state, or its DynamoDB table's schema.

---

## 7. Acceptance criteria

- AC-01: publishing `N` confirmed bookings for the same apartment within one 5-minute window
  produces exactly one `booking_pulse_5min` row with `booking_count == N`.
- AC-02: a market snapshot with `platform != null` (a channel-specific snapshot) is excluded from
  `market_pulse_5min`'s aggregation — only blended snapshots count.
- AC-03: a booking for an apartment/date with no matching `price_decision` is counted in
  `booking_count` but excluded from `avg_profit_eur`'s average, and increments
  `unresolved_cost_count` for its window.
- AC-04: a booking event arriving more than 30s out of order, but within the 1-minute allowed
  lateness, correctly updates its window's already-emitted row rather than being dropped or
  starting a new window.
- AC-05: a booking event arriving after the allowed-lateness period is routed to the late-data
  side output, not silently dropped, and does not affect any emitted window.
- AC-06: killing and restarting this job does not affect the existing pricing job's consumer
  offsets, checkpoints, or DynamoDB writes (verified by running both jobs concurrently and
  restarting only this one).
- AC-07: `avg_profit_eur` computed by this job, for a hand-picked booking with a known
  `price_decision`, matches the manually-computed formula in §4.3 to the cent.

---

## 8. Known limitations

- The profit figure is an approximation (§4.3) — a deliberate, documented trade-off, not an
  oversight.
- Async I/O introduces a dependency on DynamoDB read latency/availability for this job's
  throughput; no circuit breaker beyond `AsyncDataStream`'s own timeout/retry is implemented in
  this phase.
- Like the rest of this project's Flink usage, this job's watermark/window state is not persisted
  across job restarts beyond Flink's own checkpointing — a from-scratch restart (not a
  checkpoint-restore) loses in-flight window state, the same characteristic the existing pricing
  job already has via its keyed state.
