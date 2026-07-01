# Phase 1 — CDC Pipeline: Postgres → Debezium → Kafka

**Status:** Draft
**Depends on:** none (first buildable phase)
**Blocks:** Phase 3 (Flink processing) consumes `payment-events.v1` produced here
**Related:** [ADR-0001](../../../docs/adr/ADR-0001-kafka-kinesis-split.md), [ADR-0003](../../../docs/adr/ADR-0003-payment-line-cdc-contract.md), [`payment_line.v1.json`](../../events/payment_line.v1.json), [`payment_lines.sql`](./payment_lines.sql)

---

## 1. Executive summary (plain language)

Today, every operational cost for an apartment — electricity, water, cleaning, the OTA commission Airbnb takes, insurance — lives as a row in the property manager's existing database. Nothing currently reads that data automatically; a person would have to query it manually to know what an apartment actually costs to run this month.

Phase 1 builds the first link in the pricing engine's chain: a way to **watch that database and automatically republish every cost line, the instant it's written or updated, onto a message stream** that later phases (market comparison, the pricing calculation itself) can consume. We do this using a well-established technique called **Change Data Capture (CDC)** — instead of writing new code inside the property manager's application, we read its database's internal change log directly. This means the existing app doesn't change at all; the pricing engine observes it non-invasively.

By the end of this phase, we can prove: *"a cost is entered in Postgres, and within seconds it shows up correctly on the event stream, including corrections and historical data."* That is the foundation everything else in this project is built on — without trustworthy, complete cost data, no price recommendation can be trusted either.

This phase does **not** yet compute any prices, fetch any market data, or produce anything the dashboard would show. It is purely: get the cost data flowing, correctly and provably.

---

## 2. Scope

### In scope

- A `payment_lines` PostgreSQL table matching the contract in [`payment_lines.sql`](./payment_lines.sql).
- A Debezium PostgreSQL connector ([`infra/debezium/postgres-connector.json`](../../../infra/debezium/postgres-connector.json)) that:
  - performs an **initial snapshot** of existing rows, then
  - **streams** ongoing inserts and updates via logical replication (WAL, `pgoutput`).
- The `payment-events.v1` Kafka topic, partitioned by `apartment_id`, carrying messages that validate against [`payment_line.v1.json`](../../events/payment_line.v1.json) with **zero additional fields**.
- Seed data: enough historical rows to demonstrate the snapshot behavior and give later phases something realistic to compute against (requirements in §5; generating the actual seed script is implementation work, not this spec).
- Manual verification via `kcat` (per README) that the above holds.

### Out of scope (explicitly deferred)

- Any Flink processing, aggregation, or join (Phase 3).
- Market price ingestion (Phase 2).
- Any pricing computation or `price_decision` output.
- DynamoDB / Iceberg / dashboard (Phases 4–5).
- Automated integration tests that spin up the full Docker Compose stack (Phase 1 verification is manual via `kcat`, per the project's phased approach — automating this is a candidate for a later hardening pass, not a blocker for calling Phase 1 done).
- Handling hard deletes of `payment_lines` rows as a first-class case (see §6, Known Limitations).
- Multi-currency support (`currency` stays hardcoded to EUR — see schema).

---

## 3. Data contract

The event contract is [`specs/events/payment_line.v1.json`](../../events/payment_line.v1.json). Three properties of that contract are load-bearing for this phase specifically (full rationale in ADR-0003):

1. **No CDC envelope metadata.** The Kafka payload is the business event only — no `op`, `db`, `table`, `lsn`, or `source.ts_ms`. The connector config must not re-introduce these (regression-tested in `specs/contracts/`).
2. **`event_id` is a stable row identifier, not a per-message identifier.** It is a column on `payment_lines` (`UUID PRIMARY KEY DEFAULT gen_random_uuid()`), assigned once at insert and never reassigned. The same `event_id` recurring with different field values on a later message means "this row was updated," not "duplicate message."
3. **No nested objects.** `supplier_name`/`supplier_tax_id` and `billing_period_start`/`billing_period_end` are flat top-level fields, matching what Debezium naturally emits from a flat table row.

The full source table definition is [`payment_lines.sql`](./payment_lines.sql). It is the authoritative mapping from event schema field to Postgres column, type, and constraint — every `CHECK` constraint mirrors an `enum` or range in the JSON Schema by construction, so the two cannot silently drift without a failing `CHECK` or a failing contract test.

---

## 4. Debezium connector behavior

Config: [`infra/debezium/postgres-connector.json`](../../../infra/debezium/postgres-connector.json).

| Behavior | Setting | Why |
|---|---|---|
| Initial snapshot | `snapshot.mode: initial` | Backfills existing rows so the pricing engine has cost history from day one of the demo (see §5). Explicit, not left to Debezium's default. |
| Replication mechanism | `plugin.name: pgoutput`, `slot.name: debezium_payment_lines` | Native PostgreSQL logical decoding, no extra plugin install required (unlike `decoderbufs`/`wal2json`). |
| Publication | `publication.name: dbz_publication`, `publication.autocreate.mode: filtered` | Created explicitly by `payment_lines.sql`; `filtered` is a safety net if it's ever missing, scoped to `table.include.list` only (never "all tables"). |
| Row → event shape | `transforms.unwrap.type: ExtractNewRecordState` | Emits only the row's current ("after") state — the CDC envelope itself is discarded, which is what makes the flat, metadata-free contract in §3 possible. |
| Updates | *(no special config — this is the transform's default behavior)* | An `UPDATE` produces a new message with the row's new state and the same `event_id`. Consumers must upsert by `event_id`, per ADR-0003. |
| Deletes | `transforms.unwrap.delete.handling.mode: drop`, `transforms.unwrap.drop.tombstones: true` | A hard delete (not expected from the PM app — see §6) is silently dropped rather than propagated as an event with undefined downstream meaning. |
| Failure handling | `errors.tolerance: none` | A malformed row or connector error stops the connector rather than silently skipping data — correct for a cost pipeline feeding pricing decisions. |

---

## 5. Seed / backfill data requirements

Debezium's initial snapshot only backfills what's already in `payment_lines` when the connector starts. For the pricing engine (later phases) to compute a believable `daily_cost_eur` on day one of any demo, the table must be seeded **before** the connector is registered. This spec defines what that seed data must satisfy; writing the generator itself is Phase 1 implementation work.

Requirements:

- **Coverage:** at least 2 full calendar months of history, across at least 10 apartments (representative subset of the ~100-apartment target from the README's motivation — full 100-apartment scale is a demo-polish nice-to-have, not a Phase 1 blocker).
- **Realism:** each apartment/month should have multiple cost lines across at least 3 different `concept` values (e.g. `electricity`, `water`, `ota_fee`), with amounts in plausible EUR ranges for Spanish vacation rentals.
- **Provenance:** all seeded rows must use `source: "synthetic"` — this is precisely why that enum value exists — so seeded data is always distinguishable from anything resembling real `bank_statement`/`manual_entry` data in later testing or demos.
- **Identifiers:** `apartment_reference` should follow the `<CITY>-<NNN>` pattern already used in the schema's examples (`BCN-042`, `MAD-007`, `VLC-015`) so market-area matching in later phases has something consistent to join against.
- **Mutation coverage:** at least one seeded apartment must have a row that is later `UPDATE`d after the snapshot (e.g. `payment_status` `pending` → `paid`) to exercise the update path in acceptance criteria (§6, AC-04), not just the snapshot path.

---

## 6. Acceptance criteria

Verification method for all of these is manual via `kcat` and `psql`, per the README's existing tooling — no automated integration test harness is required for Phase 1 (see §2, Out of scope).

- **AC-01 — Snapshot backfill.** Given `payment_lines` is seeded per §5 *before* the connector is registered, when the connector starts with `snapshot.mode: initial`, then the count of messages on `payment-events.v1` equals `SELECT count(*) FROM payment_lines` once the snapshot completes.
- **AC-02 — Live insert.** Given the connector is running and caught up, when a new row is inserted into `payment_lines`, then a corresponding message appears on `payment-events.v1` within 5 seconds.
- **AC-03 — Schema conformance.** Every message on `payment-events.v1` (snapshot and streaming alike) validates against `payment_line.v1.json` via the `specs/contracts/` test suite, with no additional properties present.
- **AC-04 — Update semantics.** Given an existing row, when its `payment_status` changes from `pending` to `paid` (and `payment_date` is set), then a new message appears on `payment-events.v1` carrying the **same `event_id`** as the original insert, with `updated_at` strictly later than `created_at`.
- **AC-05 — Partitioning.** All messages for a given `apartment_id` land on the same Kafka partition (verifiable via `kcat -C -t payment-events.v1 -f 'Partition: %p\n'` grouped by a known `apartment_id`), per ADR-0001's ordering requirement.
- **AC-06 — Delete is a documented no-op.** Given a row is hard-deleted directly in Postgres, when Debezium processes the corresponding WAL delete, then **no message** appears on `payment-events.v1` — this is expected behavior (§6 limitation), not a defect to fix.
- **AC-07 — Replay safety.** Restarting the Kafka Connect worker resumes from its last committed offset without requiring manual intervention. Whether or not duplicate messages occur across the restart, every individual message still independently satisfies AC-03, and a duplicate carrying identical field values for a given `event_id` is safe for any consumer to apply twice (upsert semantics, per ADR-0003) — Phase 1 does not need to prevent duplicates, only ensure they're harmless.

---

## 7. Known limitations

- **Hard deletes are silently dropped**, not alerted on. If the PM application's write pattern ever changes to include real deletes, this pipeline will lose data invisibly. Mitigation for a production system (out of scope for this PoC) would be a periodic reconciliation job comparing `SELECT count(*) FROM payment_lines` against distinct `event_id` counts on the topic. Tracked as a residual risk, not fixed here — see ADR-0003.
- **No automated integration tests.** Acceptance criteria are verified manually via `kcat`/`psql`. This matches the project's phased, learning-focused approach but means regressions in connector behavior won't be caught by CI — only schema-contract regressions will (via `specs/contracts/`).
- **Snapshot performance is untested at ~100-apartment scale.** The seed requirement (§5) targets 10+ apartments for Phase 1 acceptance; if a full 100-apartment, multi-month seed is later needed for demo polish, initial snapshot duration should be re-verified (Debezium's snapshot is a full-table scan under a lock briefly at the start — see [Debezium docs](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-snapshots) for exact locking behavior).

## 8. Follow-ups for later phases

- **Phase 3 (Flink) must implement cost aggregation as keyed-state upsert by `event_id`**, never as a running sum — this is a hard constraint from ADR-0003, not a Phase 3 design choice to revisit.
- **Phase 3 needs a way to detect "stale" or "incomplete" cost data** (e.g. an apartment with zero cost lines in the current billing period) — not addressed here, since Phase 1 only guarantees correct transport of whatever exists in Postgres.
- Wiring `payment_lines.sql` into `infra/docker-compose.yml` (as a `docker-entrypoint-initdb.d` script) and writing the actual seed-data generator described in §5 are both Phase 1 **implementation** tasks, not specified further here.
