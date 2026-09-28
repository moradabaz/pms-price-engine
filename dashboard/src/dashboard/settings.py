from pydantic_settings import BaseSettings, SettingsConfigDict


class DashboardSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DASHBOARD_")

    dynamodb_endpoint_url: str | None = None
    aws_region: str = "eu-west-1"

    price_decision_table_name: str = "price_decision"

    # Phase 26 — Market Pulse Job's own tables, live-read here alongside
    # price_decision (same DynamoDB, same endpoint/credentials).
    market_pulse_table_name: str = "market_pulse_5min"
    bookings_created_table_name: str = "bookings_created_5min"
    bookings_cancelled_table_name: str = "bookings_cancelled_5min"

    # Same file dbt-runner writes, mounted read-only.
    duckdb_path: str = "/data/dbt/pms_lakehouse.duckdb"
    marts_cache_ttl_seconds: int = 300
    duckdb_lock_retry_attempts: int = 3
    duckdb_lock_retry_backoff_seconds: float = 1.0

    log_level: str = "INFO"
