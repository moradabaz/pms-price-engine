from pydantic_settings import BaseSettings, SettingsConfigDict


class MockAppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MOCK_APP_")

    postgres_dsn: str
    seed_months: int = 2
    seed_apartments: int = 10
    insert_interval_min_seconds: int = 10
    insert_interval_max_seconds: int = 30
    update_check_interval_seconds: int = 60
    # Phase 26 (market-pulse-job): live source of booking-events.v1
    # cancellations, so bookings_cancelled_5min has real traffic to
    # aggregate, not just the one-time seed's already-cancelled rows.
    cancellation_check_interval_seconds: int = 90
    log_level: str = "INFO"
