import multiprocessing

import duckdb
import pytest
from dashboard.settings import DashboardSettings

from dashboard import marts


@pytest.fixture
def settings(tmp_path) -> DashboardSettings:
    db_path = tmp_path / "pms_lakehouse.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("create table dim_apartment (apartment_id varchar, city varchar)")
    con.execute(
        "insert into dim_apartment values ('BCN-001', 'BCN'), ('BCN-002', 'BCN')"
    )

    con.execute(
        "create table fct_daily_price (apartment_id varchar, target_date date,"
        " suggested_price_eur double, rule_applied varchar, floor_type varchar,"
        " effective_margin double)"
    )
    con.execute(
        "insert into fct_daily_price values"
        " ('BCN-001', '2026-09-01', 120.0, 'market_competitive', 'structural', 0.2),"
        " ('BCN-001', '2026-09-02', 130.0, 'cost_protected', 'structural', 0.25)"
    )

    con.execute(
        "create table fct_price_decision (apartment_id varchar, rule_applied varchar,"
        " ingested_at timestamp)"
    )
    con.execute(
        "insert into fct_price_decision values"
        " ('BCN-001', 'market_competitive', '2026-08-06T10:00:00'),"
        " ('BCN-001', 'cost_protected', '2026-08-06T10:15:00')"
    )

    con.execute(
        "create table fct_margin_alert (apartment_id varchar, target_date date,"
        " decided_at timestamp, suggested_price_eur double, effective_margin double,"
        " floor_type varchar)"
    )
    con.execute(
        "insert into fct_margin_alert values"
        " ('BCN-001', '2026-09-02', '2026-08-06T10:15:00', 130.0, 0.25, 'structural')"
    )
    con.close()

    return DashboardSettings(duckdb_path=str(db_path))


def test_list_apartment_ids(settings):
    assert marts.list_apartment_ids(settings) == ["BCN-001", "BCN-002"]


def test_price_evolution_orders_by_target_date(settings):
    df = marts.price_evolution(settings, "BCN-001")

    assert list(df["suggested_price_eur"]) == [120.0, 130.0]


def test_margin_alerts_only_cost_protected(settings):
    df = marts.margin_alerts(settings)

    assert len(df) == 1
    assert df.iloc[0]["apartment_id"] == "BCN-001"


def test_freshness_returns_max_ingested_at(settings):
    when = marts.freshness(settings)

    assert str(when) == "2026-08-06 10:15:00"


def _hold_writer(db_path: str, ready, stop) -> None:
    con = duckdb.connect(db_path)
    ready.set()
    stop.wait(timeout=10)
    con.close()


def test_read_retries_then_raises_on_persistent_lock(settings):
    # A separate process holding a writable connection open, like
    # dbt-runner's own container — AC-05, real cross-process DuckDB lock.
    ready = multiprocessing.Event()
    stop = multiprocessing.Event()
    writer = multiprocessing.Process(
        target=_hold_writer, args=(settings.duckdb_path, ready, stop)
    )
    writer.start()
    ready.wait(timeout=10)
    retry_overrides = {
        "duckdb_lock_retry_attempts": 2,
        "duckdb_lock_retry_backoff_seconds": 0.05,
    }
    settings = settings.model_copy(update=retry_overrides)
    try:
        with pytest.raises(duckdb.OperationalError):
            marts.list_apartment_ids(settings)
    finally:
        stop.set()
        writer.join(timeout=10)
