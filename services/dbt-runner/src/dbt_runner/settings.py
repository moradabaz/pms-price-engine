from pydantic_settings import BaseSettings, SettingsConfigDict


class DbtRunnerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DBT_RUNNER_")

    project_dir: str = "/app/transform"
    profiles_dir: str = "/app/transform"

    # Confirmed 2026-08-04 (spec 05 §10): the dashboard's current-price view
    # reads DynamoDB directly (hot path) — marts only serve history/margin
    # alerts, which don't need minute-level freshness.
    run_interval_seconds: int = 900
    log_level: str = "INFO"
