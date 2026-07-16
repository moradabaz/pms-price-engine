# Phase 2 — CDC Pipeline: Postgres → Debezium → Kafka

**Status:** Draft
**Depends on:** Phase 1 (mock app + `payment_lines` table must exist, be seeded, and be actively written to)
**Blocks:** Phase 4 (Flink processing) consumes `payment-events.v1` produced here
**Related:** [ADR-0001](../../../docs/adr/ADR-0001-kafka-kinesis-split.md), [ADR-0003](../../../docs/adr/ADR-0003-payment-line-cdc-contract.md), [`payment_line.v1.json`](../../events/payment_line.v1.json), [`payment_lines.sql`](../01-mock-app-db/payment_lines.sql), [Phase 1 spec](../01-mock-app-db/spec.md)

---

## 1. Executive summary (plain language)

Today, every operational cost for an apartment — electricity, water, cleaning, the OTA commission Airbnb takes, insurance — lives as a row in the property manager's existing database. Nothing currently reads that data automatically; a person would have to query it manually to know what an apartment actually costs to run this month.

Phase 2 builds the first link in the pricing engine's chain: a way to **watch that database and automatically republish every cost line, the instant it's written or updated, onto a message stream** that later phases (market comparison, the pricing calculation itself) can consume. We do this using a well-established technique called **Change Data Capture (CDC)** — instead of writing new code inside the property manager's application, we read its database's internal change log directly. This means the existing app doesn't change at all; the pricing engine observes it non-invasively.

By the end of this phase, we can prove: *"a cost is entered in Postgres, and within seconds it shows up correctly on the event stream, including corrections and historical data."* That is the foundation everything else in this project is built on — without trustworthy, complete cost data, no price recommendation can be trusted either.

This phase does **not** yet compute any prices, fetch any market data, or produce anything the dashboard would show. It is purely: get the cost data flowing, correctly and provably.

---

## 2. Scope

### In scope

- A Debezium PostgreSQL connector ([`infra/debezium/postgres-connector.json`](../../../infra/debezium/postgres-connector.json)) reading the `payment_lines` table (created and seeded in Phase 1) that:
  - performs an **initial snapshot** of existing rows, then
  - **streams** ongoing inserts and updates via logical replication (WAL, `pgoutput`), using its own dedicated replication slot distinct from any slot created manually during Phase 1 (see Phase 1 spec, §7).
- The `payment-events.v1` Kafka topic, partitioned by `apartment_id`, carrying messages that validate against [`payment_line.v1.json`](../../events/payment_line.v1.json) with **zero additional fields**.
- Manual verification via `kcat` (per README) that the above holds.

### Out of scope (explicitly deferred)

- Defining the `payment_lines` table or generating any seed/backfill data — both are Phase 1 (mock app + DB).
- Any Flink processing, aggregation, or join (Phase 4).
- Market price ingestion (Phase 3).
- Any pricing computation or `price_decision` output.
- DynamoDB / Iceberg / dashboard (Phases 5–6).
- Automated integration tests that spin up the full Docker Compose stack (Phase 2 verification is manual via `kcat`, per the project's phased approach — automating this is a candidate for a later hardening pass, not a blocker for calling Phase 2 done).
- Handling hard deletes of `payment_lines` rows as a first-class case (see §8, Known Limitations).
- Multi-currency support (`currency` stays hardcoded to EUR — see schema).

---

## 3. Data contract

The event contract is [`specs/events/payment_line.v1.json`](../../events/payment_line.v1.json). Three properties of that contract are load-bearing for this phase specifically (full rationale in ADR-0003):

1. **No CDC envelope metadata.** The Kafka payload is the business event only — no `op`, `db`, `table`, `lsn`, or `source.ts_ms`. The connector config must not re-introduce these (regression-tested in `specs/contracts/`).
2. **`event_id` is a stable row identifier, not a per-message identifier.** It is a column on `payment_lines` (`UUID PRIMARY KEY DEFAULT gen_random_uuid()`), assigned once at insert and never reassigned. The same `event_id` recurring with different field values on a later message means "this row was updated," not "duplicate message."
3. **No nested objects.** `supplier_name`/`supplier_tax_id` and `billing_period_start`/`billing_period_end` are flat top-level fields, matching what Debezium naturally emits from a flat table row.

The full source table definition is [`payment_lines.sql`](../01-mock-app-db/payment_lines.sql) (Phase 1). It is the authoritative mapping from event schema field to Postgres column, type, and constraint — every `CHECK` constraint mirrors an `enum` or range in the JSON Schema by construction, so the two cannot silently drift without a failing `CHECK` or a failing contract test.

---

## 4. Debezium connector behavior

Config: [`infra/debezium/postgres-connector.json`](../../../infra/debezium/postgres-connector.json).

| Behavior | Setting | Why |
|---|---|---|
| Initial snapshot | `snapshot.mode: initial` | Backfills existing rows so the pricing engine has cost history from day one of the demo (seeded in Phase 1 — see §7). Explicit, not left to Debezium's default. |
| Replication mechanism | `plugin.name: pgoutput`, `slot.name: debezium_payment_lines` | Native PostgreSQL logical decoding — the same plugin used for manual inspection in Phase 1, AC-06, but via its own dedicated slot. No extra plugin install required (unlike `decoderbufs`/`wal2json`). |
| Publication | `publication.name: dbz_publication`, `publication.autocreate.mode: filtered` | Created explicitly by Phase 1's `payment_lines.sql`; `filtered` is a safety net if it's ever missing, scoped to `table.include.list` only (never "all tables"). |
| Row → event shape | `transforms.unwrap.type: ExtractNewRecordState` | Emits only the row's current ("after") state — the CDC envelope itself is discarded, which is what makes the flat, metadata-free contract in §3 possible. |
| Updates | *(no special config — this is the transform's default behavior)* | An `UPDATE` produces a new message with the row's new state and the same `event_id`. Consumers must upsert by `event_id`, per ADR-0003. |
| Deletes | `transforms.unwrap.delete.handling.mode: drop`, `transforms.unwrap.drop.tombstones: true` | A hard delete (not expected from the PM app — see §7) is silently dropped rather than propagated as an event with undefined downstream meaning. |
| Failure handling | `errors.tolerance: none` | A malformed row or connector error stops the connector rather than silently skipping data — correct for a cost pipeline feeding pricing decisions. |

---

## 5. Seed / backfill data (owned by Phase 1)

Debezium's initial snapshot only backfills what's already in `payment_lines` when the connector starts. Seeding that data — coverage, realism, provenance, and mutation-coverage requirements — is entirely [Phase 1](../01-mock-app-db/spec.md)'s responsibility (its §4 and §6); this phase's only requirement is a precondition: **Phase 1's mock app must have completed its backfill and be running its continuous generator loop before this phase's Debezium connector is registered**, so `snapshot.mode: initial` has something realistic to snapshot and keeps receiving live traffic afterward.

---

## 6. Configuration

Two operational decisions have concrete values here so implementation has no open decisions left: how the `payment-events.v1` topic gets created, and how/when the Debezium connector gets registered against Kafka Connect.

### 6.1 Topic creation

Kafka's broker-level auto-create is deliberately disabled (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`, `infra/docker-compose.yml`) — a production-realistic posture, not just PoC pedantry: auto-create means the *first client to reference a topic name* (including a typo) silently creates it with whatever cluster-wide defaults happen to be set, with no per-topic partition/replication decision ever made explicitly. That's fine to learn from once but not something to build on.

| Setting | Value | Why |
|---|---|---|
| Creation mechanism | Manual `kafka-topics --create`, run once against the `kafka` container | Matches this phase's existing manual-verification philosophy (§2, Out of scope) — no init-container or connector-side auto-creation to maintain for a PoC. |
| Partitions | `6` | Kafka only guarantees order within a partition, and `apartment_id` is the partition key (ADR-0001) — so this number is chosen once, deliberately, not left to grow organically: increasing it later rehashes every existing key (`hash(key) % partitions`), silently breaking "same `apartment_id` → same partition" for all pre-existing data. `1` would make AC-05's per-`apartment_id` partition-affinity check a no-op; `6` gives headroom toward the README's ~100-apartment eventual scale and Phase 4's Flink source parallelism, at negligible per-partition cost on a single local broker. |
| Replication factor | `1` | Single-broker local Kafka (`infra/docker-compose.yml` runs one `kafka` service) — no replicas to place. |

```bash
docker compose -f infra/docker-compose.yml exec kafka \
  kafka-topics --create \
  --topic payment-events.v1 \
  --partitions 6 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

Must be run before the connector is registered (§6.2) — with `errors.tolerance: none` (§4) and auto-create off, the connector fails outright if the topic doesn't already exist.

### 6.2 Connector registration

| Setting | Value | Why |
|---|---|---|
| Mechanism | Manual `curl` POST to Kafka Connect's REST API | Consistent with this phase's decision to skip an automated integration harness (§2) — registration is a one-time operator action, verified manually alongside the rest of §7's acceptance criteria. |
| Timing | Only after confirming, via `psql` (e.g. `SELECT count(*) FROM payment_lines;`), that Phase 1's mock app has finished its seed backfill and is running its continuous generator loop | Hard precondition from §5 — `snapshot.mode: initial` only backfills whatever exists in `payment_lines` at connector-registration time; registering early snapshots an empty or partial table and AC-01 will never reconcile. |

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @infra/debezium/postgres-connector.json \
  http://localhost:8083/connectors
```

Confirm registration succeeded — `curl http://localhost:8083/connectors/pms-payment-lines-connector/status` should report `"state": "RUNNING"` for both the connector and its task — before proceeding to AC-01.

---

## 7. Acceptance criteria

Verification method for all of these is manual via `kcat` and `psql`, per the README's existing tooling — no automated integration test harness is required for Phase 2 (see §2, Out of scope).

- **AC-01 — Snapshot backfill.** Given `payment_lines` is seeded by Phase 1 *before* the connector is registered, when the connector starts with `snapshot.mode: initial`, then the count of messages on `payment-events.v1` equals `SELECT count(*) FROM payment_lines` once the snapshot completes.
- **AC-02 — Live insert.** Given the connector is running and caught up, when a new row is inserted into `payment_lines` (by Phase 1's mock app or manually), then a corresponding message appears on `payment-events.v1` within 5 seconds.
- **AC-03 — Schema conformance.** Every message on `payment-events.v1` (snapshot and streaming alike) validates against `payment_line.v1.json` via the `specs/contracts/` test suite, with no additional properties present.
- **AC-04 — Update semantics.** Given an existing row, when its `payment_status` changes from `pending` to `paid` (and `payment_date` is set), then a new message appears on `payment-events.v1` carrying the **same `event_id`** as the original insert, with `updated_at` strictly later than `created_at`.
- **AC-05 — Partitioning.** All messages for a given `apartment_id` land on the same Kafka partition (verifiable via `kcat -C -t payment-events.v1 -f 'Partition: %p\n'` grouped by a known `apartment_id`), per ADR-0001's ordering requirement.
- **AC-06 — Delete is a documented no-op.** Given a row is hard-deleted directly in Postgres, when Debezium processes the corresponding WAL delete, then **no message** appears on `payment-events.v1` — this is expected behavior (§8 limitation), not a defect to fix.
- **AC-07 — Replay safety.** Restarting the Kafka Connect worker resumes from its last committed offset without requiring manual intervention. Whether or not duplicate messages occur across the restart, every individual message still independently satisfies AC-03, and a duplicate carrying identical field values for a given `event_id` is safe for any consumer to apply twice (upsert semantics, per ADR-0003) — Phase 2 does not need to prevent duplicates, only ensure they're harmless.

---

## 8. Known limitations

- **Hard deletes are silently dropped**, not alerted on. If the PM application's write pattern ever changes to include real deletes, this pipeline will lose data invisibly. Mitigation for a production system (out of scope for this PoC) would be a periodic reconciliation job comparing `SELECT count(*) FROM payment_lines` against distinct `event_id` counts on the topic. Tracked as a residual risk, not fixed here — see ADR-0003.
- **No automated integration tests.** Acceptance criteria are verified manually via `kcat`/`psql`. This matches the project's phased, learning-focused approach but means regressions in connector behavior won't be caught by CI — only schema-contract regressions will (via `specs/contracts/`).
- **Snapshot performance is untested at ~100-apartment scale.** Phase 1's seed requirement targets 10+ apartments; if a full 100-apartment, multi-month seed is later needed for demo polish, initial snapshot duration should be re-verified (Debezium's snapshot is a full-table scan under a lock briefly at the start — see [Debezium docs](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-snapshots) for exact locking behavior).

## 9. Follow-ups for later phases

- **Phase 4 (Flink) must implement cost aggregation as keyed-state upsert by `event_id`**, never as a running sum — this is a hard constraint from ADR-0003, not a Phase 4 design choice to revisit.
- **Phase 4 needs a way to detect "stale" or "incomplete" cost data** (e.g. an apartment with zero cost lines in the current billing period) — not addressed here, since Phase 2 only guarantees correct transport of whatever exists in Postgres.
- Wiring `payment_lines.sql` into `infra/docker-compose.yml` (as a `docker-entrypoint-initdb.d` script) is Phase 1 **implementation** work, not this phase's — see [Phase 1 spec](../01-mock-app-db/spec.md).
