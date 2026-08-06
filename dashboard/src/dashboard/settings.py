from pydantic_settings import BaseSettings, SettingsConfigDict


class DashboardSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DASHBOARD_")

    dynamodb_endpoint_url: str | None = None
    aws_region: str = "eu-west-1"

    price_decision_table_name: str = "price_decision"

    # Same file dbt-runner writes, mounted read-only.
    duckdb_path: str = "/data/dbt/pms_lakehouse.duckdb"
    marts_cache_ttl_seconds: int = 300
    duckdb_lock_retry_attempts: int = 3
    duckdb_lock_retry_backoff_seconds: float = 1.0

    log_level: str = "INFO"
