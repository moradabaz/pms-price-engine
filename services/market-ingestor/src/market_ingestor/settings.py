from pydantic_settings import BaseSettings, SettingsConfigDict


class MarketIngestorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MARKET_INGESTOR_")

    # No safe default across bare-metal / Compose / real-AWS contexts — same
    # reasoning as mock-pm-app's MOCK_APP_POSTGRES_DSN (Phase 1 spec §5.2).
    kinesis_endpoint_url: str | None = None
    kinesis_stream_name: str = "market-price-events"
    aws_region: str = "eu-west-1"
    tick_interval_seconds: int = 60
    forecast_days: int = 60
    publish_max_retries: int = 3
    publish_backoff_base_seconds: float = 0.5
    log_level: str = "INFO"
