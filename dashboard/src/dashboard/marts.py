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


def channel_pricing(
    settings: DashboardSettings, apartment_id: str, target_date: str
) -> pd.DataFrame:
    """This apartment/night's most recent channel_price_matrix (Phase 16,
    ADR-0011 backlog #2), one row per channel — empty DataFrame if no
    channel data has reached this night yet."""
    columns = [
        "platform",
        "avg_nightly_rate_eur",
        "commission_pct",
        "market_reference_price_eur",
        "minimum_price_eur",
        "rule_applied",
        "suggested_price_eur",
        "effective_margin",
    ]

    def _query(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        row = con.execute(
            "select channel_price_matrix from fct_price_decision"
            " where apartment_id = ? and target_date = ?"
            " order by decided_at desc limit 1",
            [apartment_id, target_date],
        ).fetchone()
        if row is None or not row[0]:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame(row[0])[columns]

    return _read_with_retry(settings, _query)


def freshness(settings: DashboardSettings) -> datetime | None:
    """When the cold path was last updated (max ingested_at)."""

    def _query(con: duckdb.DuckDBPyConnection) -> datetime | None:
        row = con.execute("select max(ingested_at) from fct_price_decision").fetchone()
        return row[0] if row else None

    return _read_with_retry(settings, _query)
