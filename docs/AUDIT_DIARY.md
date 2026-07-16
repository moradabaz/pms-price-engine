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

**Status:** Not started. Spec not yet written.

**What it needs to cover** (per README's one-line description and ADR-0001): a scraper/mock service publishing `market_price.v1` events to Kinesis (LocalStack locally). Unlike Phase 2, this has no CDC dependency — the producer can shape its own JSON directly, so the class of bug in Phase 2 (connector defaults silently diverging from the contract) mostly doesn't apply here; the equivalent risk is more likely in `boto3`/Kinesis client configuration (partition key choice, `endpoint_url` pointed at LocalStack vs. real AWS per ADR-0001's consequences section).

**Before starting:** write `specs/phases/03-market-ingestion/spec.md` first, following the Phase 1/Phase 2 template (executive summary → scope → data contract → behavior → configuration → acceptance criteria → known limitations → follow-ups) — this project is explicitly spec-driven, and both prior phases show real value from writing acceptance criteria *before* implementation (Phase 2's AC-05 is what caught the partitioning bug; without a written AC there'd have been no forcing function to check for it at all).

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

As of 2026-07-16, on branch `phase-2-cdc-pipeline-config` (not yet merged):

1. **Immediate next step:** get this branch reviewed and merged — it contains the fully-verified Phase 2 implementation (connector config + `custom-converters/` + `docker-compose.yml` changes + four `error-handling/` write-ups + this diary).
2. **Then: Phase 3 needs its spec written** before any implementation — nothing exists for it yet beyond the one-line README description and ADR-0001's Kinesis decision.
3. **Read `error-handling/` before modifying `infra/debezium/postgres-connector.json` again** — four non-obvious, already-paid-for gotchas live there; re-discovering any of them would be pure wasted time.
4. **Local environment reminders:** Docker Desktop must be running; `uv sync` for Python deps; `psql` and (ideally) `kcat` for manual verification per the README (this session substituted `kafka-console-consumer` inside the `kafka` container since `kcat` wasn't installed locally — either works, but `kcat`'s flags are what the specs currently document); the Maven build step above is required once per environment to reproduce the custom date converter.
5. **No automated integration tests exist for the CDC pipeline** (Phase 2 spec §2, a deliberate scope decision) — every verification in this diary and in `error-handling/` was performed manually against a live stack. If a future phase's timeline allows it, automating at least a subset of Phase 2's AC-01–AC-07 checks (topic message counts, schema validation, partition-affinity check) would catch a regression far faster than the manual process this session used.
