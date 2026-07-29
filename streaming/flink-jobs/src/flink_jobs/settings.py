from pydantic_settings import BaseSettings, SettingsConfigDict


class FlinkJobSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FLINK_JOB_")

    kafka_bootstrap_servers: str
    payment_events_topic: str = "payment-events.v1"
    apartment_segments_topic: str = "apartment-market-segments.v1"
    kafka_consumer_group_id: str = "flink-price-engine"

    kinesis_stream_name: str = "market-price-events"
    kinesis_endpoint_url: str | None = None
    aws_region: str = "eu-west-1"

    dynamodb_table_name: str = "price_decision"
    dynamodb_endpoint_url: str | None = None

    checkpoint_interval_ms: int = 60_000
    checkpoint_storage_path: str
    max_parallelism: int = 128
    parallelism: int = 4
