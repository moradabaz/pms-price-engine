# Audit Diary

A chronological, phase-by-phase record of what has actually been done in this project, why, and what's left — written so that anyone picking this up (including a future version of whoever's reading this) can get oriented without reconstructing context from commit messages and conversation history that won't be around forever.

**How to use this document:**
- Read top to bottom for full history, or jump straight to [Where things stand right now](#where-things-stand-right-now) for immediate next steps.
- Each phase section links to its spec (`specs/phases/NN-name/spec.md`), relevant ADRs (`docs/adr/`), and incident write-ups (`error-handling/`) rather than duplicating their content — this diary is the map, not the territory.
- Update this file at the end of every phase (or sooner, if a session produces a non-obvious finding worth preserving) — it decays fast if it isn't kept current.

---

## Phase 0 — Repo setup

**Status:** Done. **Commit:** `848f1ad` ("chore: initial repository setup — streaming PoC structure"), refined by `42b8a36` (pandas/apache-flink dependency conflict fix).

Established the whole project skeleton before any single phase was implemented: the uv workspace (single lockfile across all Python services), the full `docker-compose.yml` (including services — Flink, LocalStack — that later phases haven't reached yet), the JSON Schema contracts under `specs/events/`, the ADR set (`docs/adr/ADR-0001`–`ADR-0004`), and CI (lint/typecheck/schema-contract validation on every push).

Key architectural decisions made here, still binding:
- **ADR-0001:** Kafka for `payment-events` (Debezium's native target), Kinesis for `market-price-events` (no CDC dependency) — deliberately mixed buses, not unified, both keyed by their respective business ID for per-entity ordering.
- **ADR-0002:** PyFlink over Java Flink — single-language (Python) stack.
- **ADR-0004:** pandas pinned to `2.2.x` workspace-wide to satisfy both `apache-flink` and `dashboard` under one lockfile.

---

## Phase 1 — Mock app & `payment_lines` DB

**Status:** Done, merged to `main` (PR #3, merge commit `60cfb16`). **Spec:** [`specs/phases/01-mock-app-db/spec.md`](../specs/phases/01-mock-app-db/spec.md).

**What was built:** `payment_lines.sql` (the authoritative source table + `dbz_publication`, wired into `docker-compose.yml` as a `docker-entrypoint-initdb.d` script) and `services/mock-pm-app` — a small generator that seeds ≥2 months of history across ≥10 apartments, then continuously inserts new rows (10–30s interval) and periodically flips `pending` rows to `paid`, simulating a live PM back-office.

**Key decisions:**
- **ADR-0003** was written *during* this phase, after discovering the originally-drafted `payment_line.v1` schema (nested `supplier`/`billing_period` objects, envelope metadata fields) couldn't actually be emitted by a flat-table CDC connector without extra reshaping infrastructure. Settled on: flat fields only, no CDC envelope metadata, `event_id` as a stable per-row UUID that consumers must upsert-by (not sum or append). This decision is load-bearing for every later phase that touches `payment_line.v1`.
- Phase 1's own §5 (Configuration) added concrete values for the mock app's env vars (`MOCK_APP_*`, `pydantic-settings`-based) and the Compose wiring — added in commit `ebb4604`, the same pattern this diary recommends for every future phase spec (decide configuration explicitly before implementation, don't leave it implicit).

**Non-obvious finding, fixed in-flight:** `pgoutput` produces *binary* logical-decoding output — the plain-text `pg_logical_slot_get_changes()` function errors against it (`ERROR: logical decoding output plugin "pgoutput" produces binary output`). Must use `pg_logical_slot_get_binary_changes()` or `pg_recvlogical` instead. Fixed in commit `7bf9bec`, discovered by actually running the AC-06 verification against the live stack rather than assuming the spec's originally-drafted command was correct.

**Residual limitation carried forward:** the manually-created replication slot used for Phase 1's own AC-06 must be dropped before Phase 2 registers its own (`debezium_payment_lines`) — an abandoned slot makes Postgres retain WAL indefinitely.

---

## Phase 2 — CDC pipeline (Postgres → Debezium → Kafka)

**Status:** Implemented — all seven acceptance criteria verified live against the running stack on 2026-07-16. **Spec:** [`specs/phases/02-cdc-pipeline/spec.md`](../specs/phases/02-cdc-pipeline/spec.md). Not yet merged to `main` (branch `phase-2-cdc-pipeline-config`).

**What was built:** `infra/debezium/postgres-connector.json` (the Debezium PostgreSQL source connector), the `payment-events.v1` Kafka topic (6 partitions, RF 1), and — discovered as a genuine requirement partway through, not planned upfront — `infra/debezium/custom-converters/`, a small Java project implementing Debezium's `CustomConverter` SPI to fix DATE column encoding (see below). Also added spec §6 (Configuration: topic partition count, creation command, connector-registration procedure) before implementation began, following Phase 1's precedent.

**This phase's core lesson, worth internalizing before working on any future CDC connector in this repo:** a Debezium connector reporting `"state": "RUNNING"` tells you almost nothing about whether it's actually delivering correct data. Every one of the four bugs below passed a naive health check. The only checks that actually caught them were: (1) counting messages on the *data plane* (`GetOffsetShell`, a live consumer), not the control plane (REST `/status`); (2) checking the Postgres replication slot's own lag (`pg_wal_lsn_diff`) directly; and (3) programmatically validating *every* live message against the real JSON Schema, not eyeballing one. Four full write-ups, each with a "situations you can hit this" section generalizing beyond our specific instance, live in [`error-handling/`](../error-handling/) — **read them before touching this connector's config again**:

1. **[`debezium-default-topic-naming-mismatch.md`](../error-handling/debezium-default-topic-naming-mismatch.md)** — Debezium's default topic name (`<prefix>.<schema>.<table>`) never matched the required `payment-events.v1`; nothing was flowing at all despite a healthy connector. Fixed with a `RegexRouter` transform.
2. **[`debezium-heartbeat-topic-stalls-entire-connector.md`](../error-handling/debezium-heartbeat-topic-stalls-entire-connector.md)** — `heartbeat.interval.ms` pointed at an auto-named topic that didn't exist; one unresolvable topic on a connector's *shared* producer stalls delivery to every topic it writes to, not just the affected one. Fixed by removing the setting (not needed here — the source table gets real writes constantly).
3. **[`debezium-date-decimal-wire-encoding-mismatch.md`](../error-handling/debezium-date-decimal-wire-encoding-mismatch.md)** — NUMERIC columns arrived as opaque base64 bytes, DATE columns as raw epoch-day integers, neither matching the JSON Schema contract. Fixed decimals with `decimal.handling.mode: double` (one config line); fixed dates by writing and compiling a real Debezium `CustomConverter` Java class (no bundled Kafka Connect SMT recognizes Debezium's own logical types — confirmed by testing, not assumption). Includes a postscript finding of its own: Kafka Connect persists offsets independent of the connector object's lifecycle, so fixing a connector's config does **not** retroactively fix already-published messages — a full offset reset (`stop` → `DELETE .../offsets` → topic recreate → `resume`) was needed to get a fully clean, correctly-encoded topic.
4. **[`debezium-default-key-breaks-partition-affinity.md`](../error-handling/debezium-default-key-breaks-partition-affinity.md)** — Debezium's default Kafka record key is the table's primary key (`event_id`), completely unrelated to ADR-0001's "partition by `apartment_id`" ordering requirement — 10 of 12 apartments were scattered across all 6 partitions. Fixed with `message.key.columns`, which only changes the key/partition assignment, leaving the payload's `event_id` untouched for Flink's upsert semantics.

**Environment note for reproducing this locally:** the custom converter JAR is *not* committed (see `.gitignore`) — rebuild it with:
```bash
docker run --rm -v "$(pwd)/infra/debezium/custom-converters:/app" -w /app \
  maven:3.9-eclipse-temurin-17 mvn -q -DskipTests package
```
before bringing up `docker-compose.yml`'s `kafka-connect` service, which volume-mounts the built JAR into the connector's plugin directory.

---

## Phase 3 — Market ingestion

**Status:** Implemented — AC-01 through AC-06 verified live against a running LocalStack stack on 2026-07-25, both via a direct `uv run` invocation and the real `services/market-ingestor/Dockerfile` built and run through `infra/docker-compose.yml`. **Spec:** [`specs/phases/03-market-ingestion/spec.md`](../specs/phases/03-market-ingestion/spec.md). **Diagram:** [`diagrams/phase-3-market-ingestion.md`](../diagrams/phase-3-market-ingestion.md). Not yet on its own branch — built directly on `main` this session (see "Where things stand," below).

**What was built:** `services/market-ingestor` — a synthetic market-price generator publishing `market_price.v1` snapshots to the `market-price-events` Kinesis stream (4 shards, provisioned in Phase 0's `infra/localstack/init-aws.sh`) on a fixed interval, plus the first real implementation in `libs/shared-schemas` (a hand-written `MarketPrice` Pydantic model, validated against the raw JSON Schema during this session, not just against itself).

**Key decisions, discovered and resolved before implementation (spec-driven, per this project's own practice):**

- **[ADR-0005](adr/ADR-0005-market-price-partition-key.md)** — ADR-0001 required partitioning both streams by `apartment_id`, but `market_price.v1` has no such field; it's scoped by market segment (city/neighborhood/property-profile), not an individual apartment. Resolved by partitioning `market-price-events` on a plain string built from the segment (`city|neighborhood|type|bedrooms`) — no application-level hash, Kinesis hashes it internally. A coarser key (city only) was considered and rejected: shard count is fixed at stream creation, so fewer distinct key values generally *worsens* concentration rather than improving it, while giving Flink no additional locality it actually needs.
- **Log-normal price sampling, not independently-randomized percentiles** (spec §5.2) — `avg_nightly_rate`/`p25`/`p50`/`p75` are derived from a synthetic per-listing price sample drawn from a log-normal distribution (median anchored to 2025–2026 market observations researched for Barcelona/Madrid/Valencia neighborhoods), not generated as four separate random numbers, which risked incoherent output like `p75 < p25` with nothing preventing it.
- **Bounded retry + log-and-drop for `put_records` partial failures** (spec §4) — framed explicitly in Kleppmann's delivery-semantics terms: retry is at-least-once delivery (resending identical bytes, never regenerating the event), the log-and-drop fallback after exhausting retries is at-most-once and only fires as a terminal case. True exactly-once delivery isn't achievable at this layer; what's actually relied on is at-least-once delivery plus idempotent consumption (§3: Phase 4 must compare `collected_at` across events for the same segment/date, mirroring ADR-0003's pattern for `payment_line` but for a different reason — `market_price`'s `event_id` is a pure replay-dedup key, not a stable row identity like `payment_line`'s).

**Verified live, not just unit-tested:** every generated event validated against the real `specs/events/market_price.v1.json` (not just the hand-written Pydantic model); the retry logic was exercised against fake clients simulating transient and persistent `put_records` failures; and — the most valuable confirmation — reading all 4 shards back after several ticks showed a `30/30/10/20` record distribution, the ADR-0005 hot-shard prediction actually observed, not just theorized.

**Genuinely open problem surfaced for Phase 4, not solved here:** nothing in the project maps an `apartment_id` to a market segment. `payment_lines.sql` (Phase 1) only has `apartment_id`/`apartment_reference` — no city, neighborhood, or property-profile columns. Phase 4 cannot join `payment-events.v1` against `market-price-events` without resolving this first — see spec §8 for candidate approaches, none chosen yet.

---

## Phase 4 — Flink processing

**Status:** Implemented and verified live end-to-end against a running cluster (Kafka, LocalStack DynamoDB/S3, RocksDB+S3 checkpointing). **Spec:** [`specs/phases/04-flink-processing/spec.md`](../specs/phases/04-flink-processing/spec.md). **Pre-spec:** [`docs/phase-4-streaming-design-decisions.md`](phase-4-streaming-design-decisions.md) (decisions A–H). **Diagram:** [`diagrams/phase-4-flink-processing.md`](../diagrams/phase-4-flink-processing.md). Branch `phase-4-flink-processing`, PR [#7](https://github.com/moradabaz/pms-price-engine/pull/7) open, CI green (Lint & Format, Type Check, Schema Contract Tests).

**What was built:**
- [`apartment_market_segments.sql`](../specs/phases/01-mock-app-db/apartment_market_segments.sql) (Decision C.1) — the apartment→market-segment mapping Phase 3 left as an open problem. Seeded once from `mock-pm-app`'s own apartment pool, which now carries real `city`/`neighborhood`/`property_type`/`bedrooms` attributes (restricted to the 3 cities `market-ingestor` actually prices). Self-healing: `mock_pm_app.migrations` runs the same idempotent DDL at every startup, not just via `docker-entrypoint-initdb.d` — needed because Postgres only runs initdb scripts against an empty data directory, and this table didn't exist on volumes from earlier phases. Wired into `infra/debezium/postgres-connector.json`'s `table.include.list` and a second `RegexRouter` transform, publishing to `apartment-market-segments.v1`.
- `services/market-ingestor` follow-up (D.1/D.2): random per-segment `target_date` replaced with a deterministic cyclic offset shared by every segment per tick (full 60-day calendar coverage, refreshed hourly in steady state); a seasonality multiplier (verano ×1.30, hombro ×1.05, invierno ×0.85) applied before log-normal sampling.
- `streaming/flink-jobs/` — the two-stage PyFlink job itself: apartment-keyed cost aggregation + broadcast segment/margin enrichment (Stage A, `stage_cost_enrichment.py`), re-keyed into a segment-keyed cost⋈market cross-join with fan-out, a dead-man's-switch freshness watchdog, and the pricing formula (Stage B, `stage_price_decision.py`). Single sink: DynamoDB (`dynamodb_sink.py`), idempotent by `decision_id`. Runs as a real Docker image (`pms-flink-pyflink:2.3.0`, built from `streaming/flink-jobs/Dockerfile`) inside `infra/docker-compose.yml`'s `flink-jobmanager`/`flink-taskmanager`.
- `services/kinesis-kafka-bridge` — a small new service, not anticipated in the original spec: republishes `market-price-events` (Kinesis) onto a new Kafka topic, `market-price-bridge.v1`, unmodified, so Stage B can read it via `KafkaSource`. See ADR-0008 below.
- `PaymentLine` and `PriceDecision` Pydantic models added to `libs/shared-schemas`, alongside the existing `MarketPrice` — validated against every `specs/contracts/` fixture before use.

**Key decisions:**
- **[ADR-0006](adr/ADR-0006-dynamodb-single-writer-iceberg-cdc.md)** — no dual-write from Flink to both DynamoDB and Iceberg (Kleppmann's dual-write problem). Flink writes only to DynamoDB; Iceberg is populated by a separate CDC consumer reading DynamoDB Streams, owned by Phase 5.
- **[ADR-0007](adr/ADR-0007-price-decision-cost-protected-rule.md)** — `price_decision.v1`'s `rule_applied` gains a third value, `cost_protected` (the cost floor pushed the price above the raw market average, not just above the discounted reference price) — the first change to a contract an earlier phase had already treated as closed.
- **[ADR-0008](adr/ADR-0008-kinesis-kafka-bridge.md)** — discovered only once real connector JARs were wired up: PyFlink 2.3's Kinesis connector is the legacy `FlinkKinesisConsumer`, which extends `RichParallelSourceFunction`, part of the `SourceFunction` API Flink 2.0 removed — confirmed by inspecting the actual jar's bytecode, not assumed, and no released connector targets the new Source API for Kinesis yet. Resolved with `kinesis-kafka-bridge` rather than downgrading the cluster to Flink 1.20 or blocking on an unreleased connector.
- **`available_days`**, required by `price_decision.v1` since it was first drafted, was never actually resolved by any phase — nothing in this project tracks apartment availability/blocking. The Fase 4 spec (§14) makes the simplification explicit (full billing-period length) instead of leaving it an implicit assumption.

**Non-obvious findings — read before touching `streaming/flink-jobs/`, its Dockerfile, or `infra/docker-compose.yml`'s Flink services again:**
- [`flink-operational-checklist.md`](../error-handling/flink-operational-checklist.md) — 10 PyFlink gotchas, including a dead-man's-switch timer bug caught only by testing (full-precision `datetime` deadlines never matching Flink's millisecond-resolution timers) and the confirmed absence of a native Python `Sink` API (the DynamoDB writer is a `MapFunction` terminated by a Java `DiscardingSink`, moved to `...sink.legacy.DiscardingSink` in Flink 2.3 — also confirmed by inspecting the jar).
- [`flink-2x-removes-legacy-sourcefunction-breaking-flinkkinesisconsumer.md`](../error-handling/flink-2x-removes-legacy-sourcefunction-breaking-flinkkinesisconsumer.md) — the root-cause trace behind ADR-0008.
- [`pyflink-docker-image-build-gotchas.md`](../error-handling/pyflink-docker-image-build-gotchas.md) — 5 build issues (non-root user, `uv`'s managed Python install directory being unreadable at runtime, missing `build-essential`), each confirmed live, not guessed.
- [`full-pipeline-live-deployment-bugs.md`](../error-handling/full-pipeline-live-deployment-bugs.md) — 5 bugs only visible once the whole stack ran together: a fresh consumer group's `NoOffsetForPartitionException`, the `DiscardingSink` package move, a DynamoDB `Item` double-wrap, TaskManager missing AWS credentials the JobManager had, and a disappearing S3 bucket.
- [`stage-b-cost-side-fanout-can-emit-for-already-past-nights.md`](../error-handling/stage-b-cost-side-fanout-can-emit-for-already-past-nights.md) — found in a post-deployment code review: Stage B's cost-side fan-out doesn't sweep expired nights the way its market-side does, so it can (rarely, and not yet observed) emit a `price_decision` for a `target_date` already in the past. Not fixed — doing so changes the emission count `AC-03` pins, so it's a decision for the next spec revision, not a silent behavior change.

**Verified live, not just unit-tested:**
- **AC-08 (checkpointed restart):** killed the TaskManager mid-run via `docker kill`; Flink's own checkpoint API confirmed `"latest restored"` pointed at a real S3 checkpoint; `BCN-001`'s `cost_lines_count` grew across the failure (not reset, not duplicated).
- **AC-09 (freshness watchdog):** temporarily lowered `FRESHNESS_THRESHOLD_MS` to 30s, wired the previously-unconsumed `DATA_STALE_TAG` side output to a permanent `.print()` sink (a real gap — it had no consumer at all before this), and observed `data_stale` firing correctly for both "apartment" and "night" keys across all 4 parallel subtasks. Reverted the threshold to 48h afterward; `git diff` confirmed a clean revert.
- The full pipeline end-to-end: Stage A → Stage B → DynamoDB sink, writing 190+ real `price_decision` items from live Kafka/bridge data.
- 30 unit/component tests (`uv run --package flink-jobs pytest streaming/flink-jobs/tests`), including a symmetry test added during a post-deployment senior review, plus a regression test for a clock-skew edge case (`data_age_seconds` clamped to 0, since a negative value would fail `MarketInputs`' `ge=0` validation and crash the task).

**Not yet done:** a live sample of resulting nightly prices was pulled and sanity-checked for coherence (log-normal spread, `cost_protected` vs `market_competitive` mix), but the mix currently skews entirely toward `market_competitive` — under investigation, not yet root-caused in a form that's ready to commit.

---

## Phase 5 — Persistence (Iceberg + dbt)

**Status:** Not started. Spec not yet written. Consumes Phase 4's `price_decision.v1` output into S3 + Iceberg (LocalStack locally) — **via a DynamoDB Streams CDC consumer, not direct consumption of a Flink-side event stream** ([ADR-0006](adr/ADR-0006-dynamodb-single-writer-iceberg-cdc.md), confirmed 2026-07-29: Flink writes only to DynamoDB, never a second sink). This consumer's own shard-iteration/checkpoint design is an open question for this phase's spec, as is whether LocalStack's DynamoDB Streams support is actually sufficient — flagged, not verified. dbt models on top (price evolution, margin alerts, cost-vs-price comparison per README).

---

## Phase 6 — Dashboard

**Status:** Not started. Spec not yet written. Streamlit reading current prices from DynamoDB (hot path) and history from dbt/Iceberg (cold path), per README's stack table.

---

## Phase 7 — Demo & docs

**Status:** Not started. Final ADRs, architecture diagrams, lessons-learned writeup, and — per the README's cost guardrails — a real AWS demo deployment via Terraform that must be torn down (`terraform destroy`) immediately after, with AWS Budget alerts set at $5/$10 *before* touching any real AWS service.

---

## Where things stand right now

As of 2026-07-30: Phase 2 is merged to `main` (PR #4), Phase 3 is merged to `main` (PR #5). Phase 4 is implemented, deployed live, and verified end-to-end on its own branch, `phase-4-flink-processing` — PR [#7](https://github.com/moradabaz/pms-price-engine/pull/7) is open with CI green.

1. **Immediate next step: get PR #7 reviewed and merged.** Everything in the Phase 4 section above is committed — domain decisions (ADR-0006/0007/0008), the formal spec, `apartment_market_segments` + its self-healing migration and Debezium wiring, the `market-ingestor` D.1/D.2 follow-up, `kinesis-kafka-bridge`, and `streaming/flink-jobs/` itself, live-verified including AC-08 (checkpointed restart) and AC-09 (freshness watchdog).
2. **Open investigation, not yet resolved: live `rule_applied` values skew entirely toward `market_competitive`**, with none of the expected `cost_protected`/`minimum_floor` mix a manual sample of the same Postgres data predicted. Under active investigation — the leading hypothesis involves Kafka's own topic retention having already trimmed `payment-events.v1`'s older segments before this Flink consumer group's first read, which would make Stage A's cost aggregation partial (not wrong) for apartments whose invoice history predates the topic's retention window. Not yet written up as a confirmed incident.
3. **Read `error-handling/` before modifying `infra/debezium/postgres-connector.json`, `streaming/flink-jobs/`, its Dockerfile, or `infra/docker-compose.yml`'s Flink services again** — four connector-config gotchas from Phase 2, plus Phase 4's five error-handling docs (`flink-operational-checklist.md`, the Kinesis/Flink-2.x root cause behind ADR-0008, Docker build gotchas, live-deployment bugs, and the Stage B cost-side fan-out asymmetry) — re-discovering any of them would be pure wasted time.
4. **Local environment reminders:** Docker Desktop must be running; `uv sync` for Python deps; `psql`/`kcat`/AWS CLI for manual verification per the README; the Maven build step (Phase 2) is required once per environment to reproduce the custom date converter. For Phase 4's pure logic: `uv run --package flink-jobs pytest streaming/flink-jobs/tests` needs no Docker/cluster at all. To redeploy the live Flink job after a code change: rebuild `flink-jobmanager`'s image, `docker compose up -d flink-jobmanager flink-taskmanager`, then resubmit via `flink run -pyclientexec /app/.venv/bin/python3 -pyexec /app/.venv/bin/python3 -py /app/streaming/flink-jobs/src/flink_jobs/main.py` (spec §11.1 has the full command set).
5. **No automated integration tests exist for the CDC pipeline or for market ingestion** (Phase 2 spec §2, Phase 3 spec §2 — both deliberate scope decisions); Phase 4 is a partial exception — its pure business logic has full unit-test coverage (30 tests) and a component-level test against in-memory Flink-state doubles (spec §13), and its acceptance criteria (AC-01 through AC-09 so far) have all been checked live by hand. If a future phase's timeline allows it, automating at least a subset of these phases' acceptance criteria (topic/stream message counts, schema validation, partition-affinity checks) would catch a regression far faster than the manual process used so far.
