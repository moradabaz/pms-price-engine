from lakehouse_shared import IcebergCatalogSettings
from pydantic_settings import SettingsConfigDict


class MaintenanceSettings(IcebergCatalogSettings):
    model_config = SettingsConfigDict(env_prefix="LAKEHOUSE_MAINTENANCE_")

    # Confirmed 2026-08-04 (spec 05 §10): batch-shaped maintenance, coarser
    # than the consumer's own near-real-time writes.
    compaction_interval_seconds: int = 3600
    log_level: str = "INFO"
