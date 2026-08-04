import pyarrow as pa
import pytest
from lakehouse_maintenance.maintenance import compact_once
from lakehouse_maintenance.settings import MaintenanceSettings
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, NestedField, StringType

_SCHEMA = Schema(
    NestedField(1, "decision_id", StringType(), required=True),
    NestedField(2, "suggested_price_eur", DoubleType()),
)


def _build_catalog(tmp_path) -> SqlCatalog:
    return SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )


def _build_settings(tmp_path) -> MaintenanceSettings:
    return MaintenanceSettings(
        glue_database="test_db", iceberg_table_name="price_decision_raw"
    )


def test_compact_once_is_a_noop_when_table_does_not_exist(tmp_path):
    catalog = _build_catalog(tmp_path)
    settings = _build_settings(tmp_path)

    assert compact_once(catalog, settings) == 0


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_compact_once_reduces_file_count_not_row_count(tmp_path):
    # AC-06 — same row count and content, fewer distinct data files.
    catalog = _build_catalog(tmp_path)
    settings = _build_settings(tmp_path)
    catalog.create_namespace_if_not_exists(settings.glue_database)
    table = catalog.create_table(settings.iceberg_identifier, schema=_SCHEMA)

    # Three separate appends -> three small data files, mirroring how the
    # consumer writes in small batches over time.
    for i in range(3):
        batch = pa.Table.from_pylist(
            [{"decision_id": f"decision-{i}", "suggested_price_eur": 100.0 + i}],
            schema=table.schema().as_arrow(),
        )
        table.append(batch)

    files_before = len(list(table.scan().plan_files()))
    assert files_before == 3

    rows_compacted = compact_once(catalog, settings)

    table = catalog.load_table(settings.iceberg_identifier)
    files_after = len(list(table.scan().plan_files()))
    rows_after = table.scan().to_arrow().num_rows

    assert rows_compacted == 3
    assert rows_after == 3
    assert files_after < files_before
