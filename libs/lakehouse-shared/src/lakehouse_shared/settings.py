from pydantic_settings import BaseSettings


class IcebergCatalogSettings(BaseSettings):
    """Fields both lakehouse-consumer and lakehouse-maintenance need to reach
    the same Iceberg catalog/table — never instantiated directly, always via
    a subclass that sets its own `env_prefix` (e.g. `LAKEHOUSE_CONSUMER_`).

    No safe default for `s3_endpoint_url` across bare-metal / Compose /
    real-AWS contexts — same reasoning as mock-pm-app's
    MOCK_APP_POSTGRES_DSN (Phase 1 spec §5.2).
    """

    s3_endpoint_url: str | None = None
    aws_region: str = "eu-west-1"

    # Confirmed 2026-08-04: AWS Glue Data Catalog is Ultimate-tier only in
    # LocalStack — Community returns 501 on every Glue API, not just Glue
    # Jobs (spec 05 §10, pre-spec Decision C was wrong, never live-verified).
    # Local/PoC uses PyIceberg's SqlCatalog instead; catalog_sqlite_path is
    # only meaningful there. `glue_database` is kept as the logical namespace
    # name either way — it becomes the real Glue database name in Phase 7,
    # where build_catalog() swaps to GlueCatalog.
    catalog_sqlite_path: str = "/data/catalog/pms_lakehouse.db"
    glue_database: str = "pms_lakehouse"
    iceberg_table_name: str = "price_decision_raw"
    iceberg_warehouse: str = "s3://pms-lakehouse/warehouse"

    @property
    def iceberg_identifier(self) -> str:
        """The catalog-qualified table identifier PyIceberg's
        load_table()/create_table_if_not_exists() expect."""
        return f"{self.glue_database}.{self.iceberg_table_name}"
