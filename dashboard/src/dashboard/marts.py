import time
from collections.abc import Callable
from datetime import datetime
from typing import TypeVar

import duckdb
import pandas as pd

from dashboard.settings import DashboardSettings

T = TypeVar("T")


def _read_with_retry(
    settings: DashboardSettings,
    query: Callable[[duckdb.DuckDBPyConnection], T],
) -> T:
    """Opens a short-lived read-only connection, retrying on a lock conflict
    with dbt-runner's own writer (AC-05)."""
    last_error: duckdb.OperationalError | None = None
    for attempt in range(settings.duckdb_lock_retry_attempts):
        try:
            con = duckdb.connect(settings.duckdb_path, read_only=True)
            try:
                return query(con)
            finally:
                con.close()
        except duckdb.OperationalError as error:
            last_error = error
            if attempt < settings.duckdb_lock_retry_attempts - 1:
                time.sleep(settings.duckdb_lock_retry_backoff_seconds)
    assert last_error is not None
    raise last_error


def list_apartment_ids(settings: DashboardSettings) -> list[str]:
    """Known apartment ids, from dim_apartment."""
    return _read_with_retry(
        settings,
        lambda con: [
            row[0]
            for row in con.execute(
                "select apartment_id from dim_apartment order by apartment_id"
            ).fetchall()
        ],
    )


def price_evolution(settings: DashboardSettings, apartment_id: str) -> pd.DataFrame:
    """Price history for one apartment, oldest to newest."""
    return _read_with_retry(
        settings,
        lambda con: con.execute(
            "select target_date, suggested_price_eur, rule_applied, floor_type,"
            " effective_margin"
            " from fct_daily_price"
            " where apartment_id = ?"
            " order by target_date",
            [apartment_id],
        ).fetchdf(),
    )


def margin_alerts(settings: DashboardSettings) -> pd.DataFrame:
    """cost_protected decisions, most recent first."""
    return _read_with_retry(
        settings,
        lambda con: con.execute(
            "select apartment_id, target_date, decided_at, suggested_price_eur,"
            " effective_margin, floor_type"
            " from fct_margin_alert"
            " order by decided_at desc"
        ).fetchdf(),
    )


def freshness(settings: DashboardSettings) -> datetime | None:
    """When the cold path was last updated (max ingested_at)."""

    def _query(con: duckdb.DuckDBPyConnection) -> datetime | None:
        row = con.execute("select max(ingested_at) from fct_price_decision").fetchone()
        return row[0] if row else None

    return _read_with_retry(settings, _query)
