from datetime import UTC, datetime

import pytest
from lakehouse_consumer.iceberg_writer import ensure_table, merge_rows
from lakehouse_consumer.settings import ConsumerSettings
from lakehouse_consumer.transform import row_from_new_image
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)


def _build_settings(tmp_path) -> ConsumerSettings:
    return ConsumerSettings(
        glue_database="test_db",
        iceberg_table_name="price_decision_raw",
        iceberg_warehouse=f"file://{tmp_path}/warehouse",
    )


def _build_catalog(tmp_path) -> SqlCatalog:
    return SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )


@pytest.fixture
def merge_new(sample_new_image):
    def _merge_new(
        table, decision_id: str, price: float, ingested_at: datetime
    ) -> None:
        image = sample_new_image(decision_id, price)
        merge_rows(table, [row_from_new_image(image, "INSERT", ingested_at)])

    return _merge_new


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_merge_rows_is_idempotent_by_decision_id(tmp_path, merge_new):
    # AC-02 — a replayed stream record for the same decision_id (simulating
    # at-least-once delivery) must not create a duplicate row.
    catalog = _build_catalog(tmp_path)
    settings = _build_settings(tmp_path)
    table = ensure_table(catalog, settings)
    ingested_at = datetime(2026, 8, 4, 10, 0, 0, tzinfo=UTC)
    decision_id = "22222222-2222-2222-2222-222222222222"

    merge_new(table, decision_id, 130.0, ingested_at)
    merge_new(table, decision_id, 130.0, ingested_at)  # replayed, same event

    result = catalog.load_table(settings.iceberg_identifier).scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("decision_id").to_pylist() == [decision_id]


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_merge_rows_updates_row_when_decision_id_repeats_with_new_data(
    tmp_path, merge_new
):
    catalog = _build_catalog(tmp_path)
    settings = _build_settings(tmp_path)
    table = ensure_table(catalog, settings)
    ingested_at = datetime(2026, 8, 4, 10, 0, 0, tzinfo=UTC)
    decision_id = "33333333-3333-3333-3333-333333333333"

    merge_new(table, decision_id, 130.0, ingested_at)
    merge_new(table, decision_id, 999.0, ingested_at)

    result = catalog.load_table(settings.iceberg_identifier).scan().to_arrow()
    assert result.num_rows == 1
    assert result.column("output").to_pylist()[0]["suggested_price_eur"] == 999.0


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_schema_evolution_adds_column_without_rewriting_history(tmp_path, merge_new):
    # AC-05 — adding a column must not require rewriting already-written data
    # files; old rows read back with the new column null. This is the actual
    # feature that justified choosing Iceberg over plain Parquet (spec 05 §A).
    catalog = _build_catalog(tmp_path)
    settings = _build_settings(tmp_path)
    table = ensure_table(catalog, settings)
    ingested_at = datetime(2026, 8, 4, 10, 0, 0, tzinfo=UTC)
    decision_id = "44444444-4444-4444-4444-444444444444"
    merge_new(table, decision_id, 130.0, ingested_at)

    with table.update_schema() as update:
        update.add_column("channel", StringType())

    result = catalog.load_table(settings.iceberg_identifier).scan().to_arrow()
    assert result.num_rows == 1
    assert "channel" in result.column_names
    assert result.column("channel").to_pylist() == [None]


_PRE_PHASE_9_SCHEMA = Schema(
    NestedField(1, "decision_id", StringType(), required=True),
    NestedField(2, "apartment_id", StringType(), required=True),
    NestedField(5, "decided_at", TimestampType(), required=True),
    NestedField(10, "dynamodb_event_name", StringType(), required=True),
    NestedField(11, "ingested_at", TimestampType(), required=True),
    NestedField(
        8,
        "calculation",
        StructType(
            NestedField(34, "rule_applied", StringType()),
            NestedField(35, "property_attribute_factor", DoubleType()),
        ),
    ),
)


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_ensure_table_migrates_an_existing_table_missing_newer_fields(tmp_path):
    # Regression test: create_table_if_not_exists loads an existing table
    # as-is and silently ignores anything ICEBERG_SCHEMA has gained since —
    # caught live against LocalStack as a DuckDB "Could not find key
    # 'los_floor_matrix' in struct" error against a table created before
    # Phase 9 (ADR-0011 backlog #1) and never migrated. ensure_table must
    # reconcile an existing table to the current schema on every call.
    catalog = _build_catalog(tmp_path)
    settings = _build_settings(tmp_path)
    catalog.create_namespace_if_not_exists(settings.glue_database)
    catalog.create_table_if_not_exists(
        settings.iceberg_identifier,
        schema=_PRE_PHASE_9_SCHEMA,
        location=f"{settings.iceberg_warehouse}/{settings.iceberg_table_name}",
    )

    table = ensure_table(catalog, settings)

    calculation_field = next(
        f for f in table.schema().fields if f.name == "calculation"
    )
    field_names = {f.name for f in calculation_field.field_type.fields}
    assert "los_floor_matrix" in field_names
    assert "property_reference_price_eur" in field_names


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_ensure_table_is_idempotent_when_already_current(tmp_path):
    # A table already matching ICEBERG_SCHEMA must not get a new schema
    # version on every service restart — union_by_name should be a no-op.
    catalog = _build_catalog(tmp_path)
    settings = _build_settings(tmp_path)
    ensure_table(catalog, settings)

    table = ensure_table(catalog, settings)

    assert table.schema().schema_id == 0
