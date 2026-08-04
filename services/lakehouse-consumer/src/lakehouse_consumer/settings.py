from lakehouse_shared import IcebergCatalogSettings
from pydantic_settings import SettingsConfigDict


class ConsumerSettings(IcebergCatalogSettings):
    model_config = SettingsConfigDict(env_prefix="LAKEHOUSE_CONSUMER_")

    # No safe default across bare-metal / Compose / real-AWS contexts — same
    # reasoning as mock-pm-app's MOCK_APP_POSTGRES_DSN (Phase 1 spec §5.2).
    dynamodb_endpoint_url: str | None = None

    source_table_name: str = "price_decision"
    checkpoint_table_name: str = "stream_checkpoints"

    poll_interval_seconds: float = 5.0
    log_level: str = "INFO"
