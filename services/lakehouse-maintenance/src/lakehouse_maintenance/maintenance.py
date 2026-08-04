from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError

from lakehouse_maintenance.settings import MaintenanceSettings


def compact_once(catalog: Catalog, settings: MaintenanceSettings) -> int:
    """Compacts price_decision_raw by rewriting its full contents as freshly
    batched Parquet files in a single commit (spec 05 §6) — the pyiceberg
    0.8.1 stand-in for the native rewrite_data_files()/expire_snapshots()
    this workspace's pyarrow<17 pin (flink-jobs) can't upgrade to. A no-op,
    logged rather than raised, if the table doesn't exist yet (the consumer
    creates it lazily on its own first write) or is currently empty — there
    is nothing to compact either way. Returns the row count compacted."""
    try:
        table = catalog.load_table(settings.iceberg_identifier)
    except NoSuchTableError:
        return 0

    full_data = table.scan().to_arrow()
    if full_data.num_rows == 0:
        return 0

    table.overwrite(full_data)
    return int(full_data.num_rows)
