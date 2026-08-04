from lakehouse_shared import IcebergCatalogSettings, build_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType


def test_build_catalog_returns_a_working_sql_catalog(tmp_path):
    settings = IcebergCatalogSettings(
        catalog_sqlite_path=f"{tmp_path}/catalog.db",
        iceberg_warehouse=f"file://{tmp_path}/warehouse",
        glue_database="test_db",
        iceberg_table_name="a_table",
    )

    catalog = build_catalog(settings)
    catalog.create_namespace_if_not_exists(settings.glue_database)
    schema = Schema(NestedField(1, "id", StringType(), required=True))
    table = catalog.create_table_if_not_exists(
        settings.iceberg_identifier, schema=schema
    )

    assert catalog.load_table(settings.iceberg_identifier) is not None
    assert table.schema() == schema
