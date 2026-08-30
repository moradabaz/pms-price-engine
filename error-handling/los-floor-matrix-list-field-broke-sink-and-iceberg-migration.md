# Incident: adding the first list-typed field (`los_floor_matrix`) broke two things unit tests never exercised

**Phase:** 9 | **Date:** 2026-08-30 | **Component:** `streaming/flink-jobs/src/flink_jobs/dynamodb_sink.py`, `services/lakehouse-consumer/src/lakehouse_consumer/iceberg_writer.py`

Both bugs only appeared live against LocalStack, not in the unit suite (80+ tests, all green) — `calculation.los_floor_matrix` (ADR-0011 backlog #1) was this project's first `list[...]`-typed field on `PriceDecision`, and every field added before it (Phase 8's `property_attribute_factor`, ADR-0009's `commission_pct`) was a scalar, so neither gap had ever been exercised.

## 1. `DynamoDbSinkFunction` had no `list` case — every decision silently failed to write

`_python_to_dynamodb()` handles `str`/`bool`/`int`/`float`/`dict`/`None`, and raises `TypeError` for anything else. `los_floor_matrix` is a `list[dict]` after `model_dump(mode="json")` — every single decision hit `TypeError: Unsupported type for DynamoDB item: <class 'list'>` inside `_to_dynamodb_item()`, **before** the `put_item` call's own retry/error handling even started (the raise happens one line above the `try` block). The Flink job kept reporting `RUNNING` with no job-level exceptions surfaced via `/jobs/<id>/exceptions` — only visible by grepping the TaskManager's own container logs for the traceback.

**Fix:** added the missing case — `{"L": [_python_to_dynamodb(v) for v in value]}` — and a new `test_dynamodb_sink.py` (this function had **zero** test coverage before this incident) covering both the raw list case and a full `PriceDecision` with a populated `los_floor_matrix`.

## 2. `ensure_table()` never migrates an already-existing Iceberg table

`create_table_if_not_exists` loads an existing table exactly as it was created — passing an updated `ICEBERG_SCHEMA` (with the new `los_floor_matrix` field) has no effect on a table that already exists from an earlier phase. This was invisible during Phase 8's own live verification because that session started from a fully wiped volume (`docker compose down -v`), so the table was always created fresh with whatever schema was current at the time. Phase 9 didn't reset the volume (no Postgres schema change, seemed unnecessary) — the first sign was `dbt run` failing with `Binder Error: Could not find key "los_floor_matrix" in struct` reading via DuckDB's `iceberg_scan()`, even though the Flink→DynamoDB→Streams→consumer path had (by then) been fixed by #1 above and was writing real data.

**Fix:** `ensure_table()` now calls `table.update_schema().union_by_name(ICEBERG_SCHEMA)` unconditionally after `create_table_if_not_exists` — reconciles the loaded table to the code's current schema every time the service starts, adding whatever's missing (including nested struct/list fields) and committing nothing when there's no difference (confirmed idempotent: a second call left `schema_id` unchanged). Same self-healing-migration shape `mock-pm-app`'s `ensure_apartment_market_segments_schema` already established for Postgres (`ADD COLUMN IF NOT EXISTS`, run unconditionally at every startup) — applied to Iceberg for the first time. Two new tests in `test_iceberg_writer.py` cover the migration (a table missing a field gets it) and its idempotency (an up-to-date table gets no new schema version).

## What to learn from this

- **A new field's *type* can be a bigger risk than the field itself.** Every prior additive schema change in this project (ADR-0007, ADR-0009, Phase 8) was a scalar — cheap, and every serialization layer already handled scalars correctly by construction. The first *structurally new* shape (a list) found two independent layers that had implicitly assumed "every value is a scalar or a dict" and never been asked to prove otherwise.
- **`create_table_if_not_exists` is not a migration mechanism, and nothing in this codebase was one until now.** Any future field added to `ICEBERG_SCHEMA` needs `ensure_table()`'s `union_by_name` call to actually reach a table that already exists — this was silently missing since Phase 5 and only surfaced because Phase 9 happened to be the first phase whose live-verification pass didn't start from a wiped volume.
- **A green unit suite proved the formula was right, not that the pipeline could carry the result anywhere.** `decide_price_los_matrix()` had solid unit coverage before either bug was found; neither test suite exercised the DynamoDB or Iceberg serialization boundary with a non-scalar value, because nothing before this needed to.
- **"No job exceptions" is not "no errors"** — same lesson `debezium-heartbeat-topic-stalls-entire-connector.md` already drew for Kafka Connect's REST API: a per-record UDF exception inside a Flink `MapFunction` doesn't reliably show up in `/jobs/<id>/exceptions` the way a job-level failure does. Grepping the TaskManager container's own logs was the only way to see the actual traceback.

## Situations where you can hit this

- **Any future field whose type is new to a given serialization layer** — the next candidate is backlog #4's `decision_components: list[DecisionComponent]` (ADR-0011), which will exercise this exact same `dynamodb_sink.py`/`iceberg_writer.py` path again; both are now covered, but a *third* new shape (e.g. a `dict`-valued map field, or a union type) would still be untested territory.
- **Any live-verification pass that reuses an existing volume/table instead of resetting it** — a fresh-volume test (like Phase 8's) will never catch a missing-migration bug, because table creation always uses the current schema on a truly empty volume. Reusing state (deliberately, to also test the upgrade path — as this session chose to) is what surfaces it.
