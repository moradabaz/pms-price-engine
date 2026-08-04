from pyiceberg.catalog import Catalog, load_catalog

from lakehouse_shared.settings import IcebergCatalogSettings


def build_catalog(settings: IcebergCatalogSettings) -> Catalog:
    """Builds the Iceberg catalog client shared by lakehouse-consumer and
    lakehouse-maintenance. Local/PoC uses PyIceberg's SqlCatalog (SQLite) —
    confirmed 2026-08-04, live-verified that AWS Glue Data Catalog is
    Ultimate-tier only in LocalStack, not available in Community at all
    (spec 05 §10, pre-spec Decision C corrected). Storage (S3, via
    LocalStack) is unaffected — only the catalog backend changes. Phase 7
    swaps this for `load_catalog(..., type="glue", ...)` against real AWS;
    nothing in either service is catalog-specific beyond this one call."""
    return load_catalog(
        settings.glue_database,
        **{
            "type": "sql",
            "uri": f"sqlite:///{settings.catalog_sqlite_path}",
            "warehouse": settings.iceberg_warehouse,
            "s3.endpoint": settings.s3_endpoint_url,
            "s3.region": settings.aws_region,
        },
    )
