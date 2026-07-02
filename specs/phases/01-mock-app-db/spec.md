# Phase 1 — Mock App & `payment_lines` Database

**Status:** Draft
**Depends on:** Phase 0 (repo setup)
**Blocks:** Phase 2 (CDC pipeline) needs this table to exist, be seeded, and be actively written to by a running generator process
**Related:** [`payment_lines.sql`](./payment_lines.sql), [`payment_line.v1.json`](../../events/payment_line.v1.json), [ADR-0003](../../../docs/adr/ADR-0003-payment-line-cdc-contract.md), [Phase 2 spec](../02-cdc-pipeline/spec.md)

---

## 1. Executive summary (plain language)

There is no real property-manager application in this project — that's normal for a PoC, but it means we need a stand-in that behaves believably enough to learn CDC against. This phase builds that stand-in: a `payment_lines` table in PostgreSQL, plus a small **mock app** that writes to it the way a real PM back-office would — new cost lines arriving continuously (invoices entered as they come in) and existing lines occasionally changing (a payment moves from `pending` to `paid`).

This phase is also the first hands-on encounter with the mechanism Phase 2 depends on: **PostgreSQL's write-ahead log (WAL)**. Before Debezium and Kafka Connect are in the picture at all, the goal here is to understand *why* logical replication works — `wal_level=logical`, a replication slot, and what a raw decoded change actually looks like — by inspecting it directly (`psql` + `pg_recvlogical` or `pg_logical_slot_get_changes`), not through a connector's abstraction.

By the end of this phase we can prove: *"Postgres is running with logical replication enabled, `payment_lines` has realistic historical and live data, and we can see raw WAL change events for both inserts and updates using nothing but Postgres tooling."* Phase 2 then plugs Debezium into exactly this.

This phase does **not** touch Kafka, Debezium, or Kafka Connect at all (see Phase 2).

---

## 2. Scope

### In scope

- The `payment_lines` PostgreSQL table, per [`payment_lines.sql`](./payment_lines.sql), wired into `infra/docker-compose.yml` as a `docker-entrypoint-initdb.d` init script (this was drafted but not yet wired — see file history).
- Enabling logical replication: `wal_level=logical` on the Postgres container, and manually creating a replication slot to confirm decoding works (`pgoutput` plugin — the same one Phase 2's Debezium connector will use).
- A minimal **mock app** (a small long-running Python process, e.g. `services/mock-pm-app`) that:
  - performs an initial **backfill/seed** of historical rows (requirements in §4), then
  - **continuously** inserts new rows on an interval and occasionally **updates** existing rows, for as long as it runs — this is what makes it a believable stand-in for a live PM app, not a one-shot script.
- Manual verification that WAL changes are observable via raw Postgres tooling (`pg_recvlogical` or `SELECT * FROM pg_logical_slot_get_changes(...)`), independent of Debezium.

### Out of scope (explicitly deferred)

- Debezium, Kafka Connect, and the `payment-events.v1` Kafka topic entirely (Phase 2).
- Market price ingestion (Phase 3).
- Any pricing computation, Flink processing, or dashboard (Phases 4–6).
- A realistic PM application UI or business logic beyond "write plausible rows on a timer" — this is a data generator, not a product.

---

## 3. Data contract

Unchanged from the original draft of this table: [`payment_lines.sql`](./payment_lines.sql) is the authoritative schema, and every column maps 1:1 to a field in [`payment_line.v1.json`](../../events/payment_line.v1.json) (full rationale in ADR-0003). This phase owns that file; Phase 2 only consumes the table it creates.

---

## 4. Mock app behavior

The generator must produce data realistic enough for later phases (cost aggregation, pricing) to compute something meaningful against, and varied enough to exercise both the insert and update paths Phase 2 needs to validate.

- **Backfill (run once, before the connector in Phase 2 is ever registered):** at least 2 full calendar months of history, across at least 10 apartments, with multiple cost lines per apartment/month spanning at least 3 `concept` values (e.g. `electricity`, `water`, `ota_fee`) at plausible EUR amounts for Spanish vacation rentals. `apartment_reference` follows the `<CITY>-<NNN>` pattern (`BCN-042`, `MAD-007`, `VLC-015`) already used in the schema's examples.
- **Continuous generation (runs indefinitely while the mock app is up):** inserts a new row every ~10–30 seconds (interval configurable), simulating invoices being entered in real time.
- **Updates:** periodically picks an existing `pending` row and flips it to `paid` (setting `payment_date`), to exercise Postgres's `UPDATE` → WAL path — not just `INSERT`.
- **Provenance:** every row the mock app writes uses `source: 'synthetic'`, per the schema's enum — never `bank_statement`/`manual_entry`, so synthetic data is always distinguishable.
- The mock app talks to Postgres directly (plain SQL/ORM inserts) — it has no awareness of Kafka, Debezium, or any later-phase component.

---

## 5. Acceptance criteria

- **AC-01 — Logical replication enabled.** `SHOW wal_level;` reports `logical` on the running Postgres container.
- **AC-02 — Table live.** `payment_lines` exists per `payment_lines.sql`, created via Docker Compose init (not a manual `psql` step).
- **AC-03 — Backfill present.** After the mock app's seed step, `payment_lines` contains ≥2 months of history across ≥10 apartments and ≥3 `concept` values, all with `source = 'synthetic'`.
- **AC-04 — Continuous writes.** With the mock app running, `SELECT count(*)` on `payment_lines` increases over a short observation window (e.g. 2 minutes) without manual intervention.
- **AC-05 — Update path exercised.** At least one row transitions `payment_status: pending → paid` (with `payment_date` set and `updated_at` advancing) while the mock app runs, independent of the initial seed.
- **AC-06 — Raw WAL visibility.** A manually created logical replication slot (`pgoutput` plugin) shows decoded change events for both an `INSERT` and an `UPDATE` against `payment_lines`, inspected directly via `pg_recvlogical` or `pg_logical_slot_get_changes` — this is the phase's core learning checkpoint and must be demonstrated before starting Phase 2.
- **AC-07 — Decoupled from Phase 2.** The mock app runs correctly with Kafka/Debezium/Kafka Connect stopped or absent entirely — it has no dependency on them.

---

## 6. Known limitations

- The mock app's data distribution is arbitrary/random, not modeled on real PM cost patterns — it's realistic enough to be useful for later phases, not a statistically faithful simulation.
- The manually created replication slot used for AC-06 must be dropped (or reused deliberately) before Phase 2 registers Debezium's own slot (`debezium_payment_lines`, per the Phase 2 spec) — two slots reading the same WAL is fine, but an abandoned unused slot will make PostgreSQL retain WAL indefinitely and should not be left running.
- No failure/retry handling in the generator — if it crashes, it's restarted manually (or via Docker Compose `restart: unless-stopped`), not a concern for a learning PoC.

---

## 7. Follow-ups for later phases

- **Phase 2 depends on this phase's mock app being started (and having seeded data) *before* the Debezium connector is registered**, so its `snapshot.mode: initial` backfill has something to snapshot — this is the same seed-timing requirement previously drafted directly in the Phase 2 spec; it now lives here since this phase owns the data generator.
- **Phase 2 must create its own dedicated replication slot** (`debezium_payment_lines`) rather than reusing whatever slot was created for this phase's AC-06 — see Known Limitations above.
