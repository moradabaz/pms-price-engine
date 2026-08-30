from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import In
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform

from lakehouse_consumer.schema import DECIDED_AT_FIELD_ID, ICEBERG_SCHEMA
from lakehouse_consumer.settings import ConsumerSettings


def ensure_table(catalog: Catalog, settings: ConsumerSettings) -> Table:
    """Creates the raw table (and its Glue database) if this is the first
    run, otherwise loads the existing one. Partitioned by days(decided_at)
    (spec 05 §4/§10, pre-spec Decision B) — Iceberg's hidden partitioning,
    no partition value computed by this consumer.

    Self-healing schema migration (Phase 9, ADR-0011 backlog #1): on an
    existing table, create_table_if_not_exists loads it as-is and ignores
    any fields ICEBERG_SCHEMA has gained since it was first created —
    union_by_name reconciles the two on every call, adding whatever's
    missing (nested struct/list fields included) and committing nothing
    when there's no difference. Same self-healing-migration shape
    mock-pm-app's ensure_apartment_market_segments_schema already
    establishes for Postgres (ADD COLUMN IF NOT EXISTS, run unconditionally
    at every startup), applied to Iceberg. Returns the table."""
    catalog.create_namespace_if_not_exists(settings.glue_database)
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=DECIDED_AT_FIELD_ID,
            field_id=1000,
            transform=DayTransform(),
            name="decided_at_day",
        )
    )
    location = f"{settings.iceberg_warehouse}/{settings.iceberg_table_name}"
    table = catalog.create_table_if_not_exists(
        settings.iceberg_identifier,
        schema=ICEBERG_SCHEMA,
        location=location,
        partition_spec=partition_spec,
    )
    with table.update_schema() as update:
        update.union_by_name(ICEBERG_SCHEMA)
    return table


def merge_rows(table: Table, rows: list[dict[str, Any]]) -> None:
    """Merges rows into the table by decision_id — delete any existing rows
    sharing a decision_id, then append the new batch, all in the same call.
    Not pyiceberg's newer Table.upsert() (unavailable on the 0.8.x line this
    workspace is pinned to, spec 05 §6) but equivalent: a crash between the
    delete and the append leaves the checkpoint unadvanced (spec 05 §5), so
    a retry redoes both — delete-of-already-deleted is a no-op, append is
    naturally safe to repeat since it re-derives from the same source event,
    never accumulates because the delete always runs first. Does nothing if
    rows is empty (delete-filter on zero IDs would otherwise be needless)."""
    if not rows:
        return
    decision_ids = [row["decision_id"] for row in rows]
    arrow_schema = schema_to_pyarrow(table.schema())
    arrow_table = pa.Table.from_pylist(rows, schema=arrow_schema)
    table.delete(delete_filter=In("decision_id", decision_ids))
    table.append(arrow_table)
