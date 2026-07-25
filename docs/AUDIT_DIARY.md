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

**Status:** Not started. Spec not yet written.

**Hard constraints already decided, carried into whatever gets built here (not open questions):**
- Cost aggregation **must** be implemented as keyed-state upsert by `event_id`, never as a running sum over incoming messages — a direct consequence of ADR-0003 and how Phase 2's connector legitimately re-delivers the same `event_id` with updated field values.
- Needs a way to detect "stale" or "incomplete" cost data (e.g., an apartment with zero cost lines in the current billing period) — flagged as an open problem in Phase 2's spec §9, not solved there.
- Must consume from two different sources (Kafka connector output + Kinesis connector output, once Phase 3 exists) — PyFlink 2.x supports both.

---

## Phase 5 — Persistence (Iceberg + dbt)

**Status:** Not started. Spec not yet written. Will consume Phase 4's `price_decision.v1` output into S3 + Iceberg (LocalStack locally), with dbt models on top (price evolution, margin alerts, cost-vs-price comparison per README).

---

## Phase 6 — Dashboard

**Status:** Not started. Spec not yet written. Streamlit reading current prices from DynamoDB (hot path) and history from dbt/Iceberg (cold path), per README's stack table.

---

## Phase 7 — Demo & docs

**Status:** Not started. Final ADRs, architecture diagrams, lessons-learned writeup, and — per the README's cost guardrails — a real AWS demo deployment via Terraform that must be torn down (`terraform destroy`) immediately after, with AWS Budget alerts set at $5/$10 *before* touching any real AWS service.

---

## Where things stand right now

As of 2026-07-25, Phase 2 is merged to `main` (PR #4), and Phase 3 is implemented and verified live — **but built directly on `main`, not on its own branch**, unlike every prior phase (Phase 1: `restructure-phase-1-mock-app-db`, PR #3; Phase 2: `phase-2-cdc-pipeline-config`, PR #4). This is a deviation from the project's own established workflow, not a deliberate decision — flagged here so it isn't silently repeated.

1. **Immediate next step:** decide how to reconcile Phase 3's work with the branch-per-phase convention — options are (a) move the uncommitted Phase 3 changes to a new `phase-3-market-ingestion` branch before committing anything to `main`, or (b) commit directly and treat this session as an explicit, acknowledged exception. Not decided as of this writing.
2. **Then: Phase 4 (Flink processing) needs its spec written.** Its hard constraints were already decided in earlier phases (see Phase 4 section above), but it also inherits a genuinely open problem Phase 3 surfaced and did not solve: **nothing in the project maps an `apartment_id` to a market segment** (`payment_lines.sql` has no city/neighborhood/property-profile columns) — this must be resolved before Phase 4's spec can even be drafted, let alone implemented. See `specs/phases/03-market-ingestion/spec.md` §8 for candidate approaches.
3. **Read `error-handling/` before modifying `infra/debezium/postgres-connector.json` again** — four non-obvious, already-paid-for gotchas live there; re-discovering any of them would be pure wasted time.
4. **Local environment reminders:** Docker Desktop must be running; `uv sync` for Python deps; `psql` and (ideally) `kcat` for manual verification per the README (this session substituted `kafka-console-consumer` inside the `kafka` container since `kcat` wasn't installed locally — either works, but `kcat`'s flags are what the specs currently document); the Maven build step above is required once per environment to reproduce the custom date converter. For Phase 3's Kinesis side, the AWS CLI (`aws --endpoint-url=http://localhost:4566 kinesis ...`) played the same manual-verification role `kcat`/`psql` played for Phase 2.
5. **No automated integration tests exist for the CDC pipeline or for market ingestion** (Phase 2 spec §2, Phase 3 spec §2 — both deliberate scope decisions) — every verification in this diary and in `error-handling/` was performed manually against a live stack. If a future phase's timeline allows it, automating at least a subset of these phases' acceptance criteria (topic/stream message counts, schema validation, partition-affinity checks) would catch a regression far faster than the manual process used so far.
