from pydantic_settings import BaseSettings, SettingsConfigDict


class MarketPulseJobSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MARKET_PULSE_JOB_")

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_consumer_group_id: str = "market-pulse-job"

    market_price_topic: str = "market-price-bridge.v1"
    booking_events_topic: str = "booking-events.v1"

    # Spec §5.2 — this job's own DynamoDB tables.
    market_pulse_table: str = "market_pulse_5min"
    bookings_created_table: str = "bookings_created_5min"
    bookings_cancelled_table: str = "bookings_cancelled_5min"
    aws_region: str = "eu-west-1"
    dynamodb_endpoint_url: str | None = None

    # Bloque 2 — market pulse window size (spec §4.1).
    market_pulse_window_minutes: int = 5

    # Global default (Bloque 0, tarea 2). booking-events.v1 overrides this
    # to 1 on its own operator (spec §3.1) — only 1 real Kafka partition.
    parallelism: int = 4
    max_parallelism: int = 128

    checkpoint_interval_ms: int = 60_000
    checkpoint_storage_path: str = "s3://pms-iceberg/checkpoints/market-pulse-job"
    s3_endpoint_url: str | None = None

    # Watermark: bounded out-of-orderness, same bound on both sources
    # (spec §3), plus per-source idleness (spec §3.1).
    watermark_out_of_orderness_seconds: int = 30
    booking_idleness_seconds: int = 60
    market_pulse_idleness_seconds: int = 120

    log_level: str = "INFO"
