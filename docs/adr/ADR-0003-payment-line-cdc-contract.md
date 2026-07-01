# ADR-0003 — Shape of the `payment_line.v1` CDC contract

**Date:** 2026-07-01
**Status:** Accepted

## Context

`payment_line.v1.json` (`specs/events/payment_line.v1.json`) is the contract for messages on the `payment-events.v1` Kafka topic. Unlike `market_price.v1` and `price_decision.v1` — both produced by application code (the market-ingestor service, and the Flink job respectively) that can shape arbitrary JSON — `payment_line.v1` is produced entirely by Debezium reading a flat PostgreSQL table via CDC. That constraint surfaced three concrete conflicts between the originally-drafted schema/connector and what Debezium can actually emit, discovered while writing the Phase 1 (CDC pipeline) spec:

1. **Envelope metadata.** The connector config (`infra/debezium/postgres-connector.json`) had `transforms.unwrap.add.fields: "op,db,table,lsn,source.ts_ms"`, which injects those fields into every message. The schema has `additionalProperties: false` and does not declare them — every real message would fail validation as originally written.
2. **Row mutability.** The PM application updates `payment_lines` rows in place (e.g. `payment_status` flips `pending` → `paid` on the same row). Debezium therefore emits multiple messages for the same logical row over its lifetime, all carrying the same `event_id`. The schema's existing description of `event_id` ("idempotency key... used to deduplicate on replay") describes replay-safety, not update semantics — it does not say what a *consumer* should do when the same `event_id` legitimately recurs with different field values.
3. **Nested objects vs. flat rows.** The schema had `supplier: {name, tax_id}` and `billing_period: {start, end}` as nested objects. A plain relational table row, passed through Debezium's `ExtractNewRecordState` transform, is flat — there is no native way to emit nested JSON sub-objects from table columns without an additional reshaping stage.

## Decision

`payment_line.v1` messages on Kafka are **the pure, flat business event — nothing else**:

1. **No CDC envelope metadata** in the payload. `transforms.unwrap.add.fields` is removed from the connector config. If a consumer needs to know this was an insert vs. an update, it infers that from application state (see #2), not from an `op` field.
2. **`event_id` identifies a row, not a message version.** It is a stable UUID assigned once per `payment_lines` row (a Postgres column, not something Debezium synthesizes) and never changes across that row's updates. Consumers (the Phase 3 Flink job) must treat every message as an **upsert of the full current state, keyed by `event_id`** — not as an additive event to sum. This makes the aggregation correct under both real updates and at-least-once replay: re-applying the same `event_id` with identical values is a harmless no-op overwrite.
3. **Nested objects are flattened at the schema level**, not reshaped in a new infra component: `supplier` → `supplier_name` + `supplier_tax_id`; `billing_period` → `billing_period_start` + `billing_period_end`. Both stay independently nullable.
4. **Hard deletes are out of scope for v1.** The PM application is expected to record corrections as new negative-amount lines (standard accounting practice — cost lines are an audit trail), never by deleting a row. The connector sets `transforms.unwrap.delete.handling.mode: "drop"` and `transforms.unwrap.drop.tombstones: "true"` — a hard delete, if it ever happens, is silently dropped from the stream rather than propagated as an ambiguous "removal" event with undefined downstream meaning.

## Rationale

- **Consistent with ADR-0001's "no unnecessary hop" principle.** Reshaping flat CDC rows into nested JSON, or re-injecting envelope metadata to match a schema written before the connector config was checked, would both require new infrastructure (a Kafka Streams/ksqlDB step, or custom SMTs) purely to preserve a JSON shape — with no stream-processing learning value and a new failure mode.
- **`event_id`-as-row-identity is the only mutability model that survives Flink checkpoint replay for free.** Any scheme where `event_id` changed per update (e.g. per-message UUID) would make deduplication and update detection two separate, harder problems; keyed upsert collapses them into one.
- **Silently dropping unexpected deletes is a deliberate, disclosed trade-off**, not an oversight: correctness (never silently double-count or misinterpret a delete) was prioritized over completeness (handling a case the domain says shouldn't occur). See Known Limitations in `specs/phases/01-cdc-pipeline/spec.md`.

## Consequences

- `specs/events/payment_line.v1.json` no longer matches its original nested shape — any code or documentation written against the earlier draft must be updated. No implementation existed yet at the time of this decision, so there is no migration cost.
- Phase 3 (Flink processing) **must** implement cost aggregation as keyed state upsert by `event_id`, not as a running sum over incoming messages. This is a hard constraint carried forward from this ADR, not a Phase 3 implementation detail to be decided later.
- If the PM application's write pattern ever changes to include hard deletes, this ADR must be revisited — the current connector config will silently discard that signal.
- `specs/contracts/` includes regression fixtures that encode points 1 and 3 directly (a fixture with leaked CDC metadata, and one with the old nested `supplier`/`billing_period` shape) so a future accidental regression to the pre-ADR-0003 shape fails CI immediately.
