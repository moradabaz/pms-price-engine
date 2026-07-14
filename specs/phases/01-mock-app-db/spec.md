# Phase 1 — Mock App & `payment_lines` Database

**Status:** Draft
**Depends on:** Phase 0 (repo setup)
**Blocks:** Phase 2 (CDC pipeline) needs this table to exist, be seeded, and be actively written to by a running generator process
**Related:** [`payment_lines.sql`](./payment_lines.sql), [`payment_line.v1.json`](../../events/payment_line.v1.json), [ADR-0003](../../../docs/adr/ADR-0003-payment-line-cdc-contract.md), [Phase 2 spec](../02-cdc-pipeline/spec.md)

---

## 1. Executive summary (plain language)

There is no real property-manager application in this project — that's normal for a PoC, but it means we need a stand-in that behaves believably enough to learn CDC against. This phase builds that stand-in: a `payment_lines` table in PostgreSQL, plus a small **mock app** that writes to it the way a real PM back-office would — new cost lines arriving continuously (invoices entered as they come in) and existing lines occasionally changing (a payment moves from `pending` to `paid`).

This phase is also the first hands-on encounter with the mechanism Phase 2 depends on: **PostgreSQL's write-ahead log (WAL)**. Before Debezium and Kafka Connect are in the picture at all, the goal here is to understand *why* logical replication works — `wal_level=logical`, a replication slot, and what a raw decoded change actually looks like — by inspecting it directly (`psql` + `pg_recvlogical` or `pg_logical_slot_get_binary_changes`), not through a connector's abstraction.

By the end of this phase we can prove: *"Postgres is running with logical replication enabled, `payment_lines` has realistic historical and live data, and we can see raw WAL change events for both inserts and updates using nothing but Postgres tooling."* Phase 2 then plugs Debezium into exactly this.

This phase does **not** touch Kafka, Debezium, or Kafka Connect at all (see Phase 2).

---

## 2. Scope

### In scope

- The `payment_lines` PostgreSQL table, per [`payment_lines.sql`](./payment_lines.sql), wired into `infra/docker-compose.yml` as a `docker-entrypoint-initdb.d` init script.
- Enabling logical replication: `wal_level=logical` on the Postgres container, and manually creating a replication slot to confirm decoding works (`pgoutput` plugin — the same one Phase 2's Debezium connector will use).
- A minimal **mock app** (a small long-running Python process, e.g. `services/mock-pm-app`) that:
  - performs an initial **backfill/seed** of historical rows (requirements in §4), then
  - **continuously** inserts new rows on an interval and occasionally **updates** existing rows, for as long as it runs — this is what makes it a believable stand-in for a live PM app, not a one-shot script.
- Manual verification that WAL changes are observable via raw Postgres tooling (`pg_recvlogical` or `SELECT * FROM pg_logical_slot_get_binary_changes(...)`), independent of Debezium. `pgoutput` produces binary protocol output, so the plain-text `pg_logical_slot_get_changes` errors against it (`ERROR: logical decoding output plugin "pgoutput" produces binary output`) — the `_binary_` variant (or `pg_recvlogical`) is required.

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

## 5. Configuration

Two things need concrete values before this phase can be implemented: how `payment_lines.sql` gets wired into Compose, and how the mock app itself is configured and deployed. Both are captured here so implementation has no open decisions left.

### 5.1 Postgres init wiring

| Setting | Value | Why |
|---|---|---|
| Init script mount | `../specs/phases/01-mock-app-db/payment_lines.sql:/docker-entrypoint-initdb.d/01-payment_lines.sql:ro` added to the `postgres` service's `volumes:` in `infra/docker-compose.yml` | Mounted directly from its spec location (single source of truth — this file is already documented as authoritative, see ADR-0003) rather than copied into an `infra/postgres/init/` directory, which would create two copies that can silently drift. |
| WAL settings | Already set on the `postgres` service: `wal_level=logical`, `max_replication_slots=4`, `max_wal_senders=4` | Done in Phase 0 — no change needed here, just confirmed by AC-01. |

### 5.2 Mock app service (`services/mock-pm-app`)

Settings are loaded via `pydantic-settings` (prefix `MOCK_APP_`), consistent with `libs/common`'s existing `pydantic-settings` dependency — this is the first real consumer of that library.

| Env var | Default | Why |
|---|---|---|
| `MOCK_APP_POSTGRES_DSN` | *(required, no default)* | e.g. `postgresql://pms:pms@postgres:5432/pms_db` inside the Compose network. No safe default exists across dev vs. Compose contexts, so it must be set explicitly. |
| `MOCK_APP_SEED_MONTHS` | `2` | Matches AC-03's minimum history window. |
| `MOCK_APP_SEED_APARTMENTS` | `10` | Matches AC-03's minimum apartment count; references are drawn from a fixed city-code pool (`BCN`, `MAD`, `VLC`, `SEV`, ...) formatted `<CITY>-<NNN>`. |
| `MOCK_APP_INSERT_INTERVAL_MIN_SECONDS` | `10` | Lower bound of §4's "every ~10–30 seconds" cadence. |
| `MOCK_APP_INSERT_INTERVAL_MAX_SECONDS` | `30` | Upper bound; the actual interval is randomized uniformly between min and max on every tick, per §4's "interval configurable." |
| `MOCK_APP_UPDATE_CHECK_INTERVAL_SECONDS` | `60` | How often the generator looks for an existing `pending` row to flip to `paid` (AC-05). Decoupled from the insert cadence because updates model back-office processing catching up, not invoices arriving. |
| `MOCK_APP_LOG_LEVEL` | `INFO` | Via `libs/common`'s `structlog` setup. |

Docker Compose addition (new service, alongside `postgres`, in `infra/docker-compose.yml`):

```yaml
mock-pm-app:
  build:
    context: ..
    dockerfile: services/mock-pm-app/Dockerfile
  container_name: pms_mock_pm_app
  depends_on:
    postgres:
      condition: service_healthy
  environment:
    MOCK_APP_POSTGRES_DSN: postgresql://pms:pms@postgres:5432/pms_db
  restart: unless-stopped
```

The build context is the repo root, not `services/mock-pm-app` itself — this service is a uv workspace member and its build needs sibling packages (`libs/common`, `libs/shared-schemas`) plus the root lockfile, none of which are reachable from a build context scoped to its own directory.

`restart: unless-stopped` directly implements §7's "no failure/retry handling... restarted manually (or via Docker Compose `restart: unless-stopped`)" — making that the actual configured behavior rather than just a documented option.

---

## 6. Acceptance criteria

- **AC-01 — Logical replication enabled.** `SHOW wal_level;` reports `logical` on the running Postgres container.
- **AC-02 — Table live.** `payment_lines` exists per `payment_lines.sql`, created via Docker Compose init (not a manual `psql` step).
- **AC-03 — Backfill present.** After the mock app's seed step, `payment_lines` contains ≥2 months of history across ≥10 apartments and ≥3 `concept` values, all with `source = 'synthetic'`.
- **AC-04 — Continuous writes.** With the mock app running, `SELECT count(*)` on `payment_lines` increases over a short observation window (e.g. 2 minutes) without manual intervention.
- **AC-05 — Update path exercised.** At least one row transitions `payment_status: pending → paid` (with `payment_date` set and `updated_at` advancing) while the mock app runs, independent of the initial seed.
- **AC-06 — Raw WAL visibility.** A manually created logical replication slot (`pgoutput` plugin) shows decoded change events for both an `INSERT` and an `UPDATE` against `payment_lines`, inspected directly via `pg_recvlogical` or `pg_logical_slot_get_binary_changes` (`pgoutput` is binary-only — the plain-text `pg_logical_slot_get_changes` does not work against it) — this is the phase's core learning checkpoint and must be demonstrated before starting Phase 2.
- **AC-07 — Decoupled from Phase 2.** The mock app runs correctly with Kafka/Debezium/Kafka Connect stopped or absent entirely — it has no dependency on them.

---

## 7. Known limitations

- The mock app's data distribution is arbitrary/random, not modeled on real PM cost patterns — it's realistic enough to be useful for later phases, not a statistically faithful simulation.
- The manually created replication slot used for AC-06 must be dropped (or reused deliberately) before Phase 2 registers Debezium's own slot (`debezium_payment_lines`, per the Phase 2 spec) — two slots reading the same WAL is fine, but an abandoned unused slot will make PostgreSQL retain WAL indefinitely and should not be left running.
- No failure/retry handling in the generator — if it crashes, it's restarted manually (or via Docker Compose `restart: unless-stopped`), not a concern for a learning PoC.

---

## 8. Follow-ups for later phases

- **Phase 2 depends on this phase's mock app being started (and having seeded data) *before* the Debezium connector is registered**, so its `snapshot.mode: initial` backfill has something to snapshot — this is the same seed-timing requirement previously drafted directly in the Phase 2 spec; it now lives here since this phase owns the data generator.
- **Phase 2 must create its own dedicated replication slot** (`debezium_payment_lines`) rather than reusing whatever slot was created for this phase's AC-06 — see Known Limitations above.
