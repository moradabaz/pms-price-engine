from pydantic_settings import BaseSettings, SettingsConfigDict


class BridgeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BRIDGE_")

    kinesis_endpoint_url: str | None = None
    kinesis_stream_name: str = "market-price-events"
    aws_region: str = "eu-west-1"

    kafka_bootstrap_servers: str
    kafka_topic: str = "market-price-bridge.v1"

    poll_interval_seconds: float = 5.0
    log_level: str = "INFO"
