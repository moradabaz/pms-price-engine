from dataclasses import dataclass
from datetime import date, datetime

from flink_jobs.eviction import (
    expired_night_keys,
    is_over_capacity,
    oldest_key_by_updated_at,
)


@dataclass
class _Entry:
    updated_at: datetime


def test_is_over_capacity():
    assert is_over_capacity(501, cap=500) is True
    assert is_over_capacity(500, cap=500) is False


def test_oldest_key_by_updated_at():
    entries = {
        "a": _Entry(datetime(2026, 1, 2)),
        "b": _Entry(datetime(2026, 1, 1)),
        "c": _Entry(datetime(2026, 1, 3)),
    }
    assert oldest_key_by_updated_at(entries) == "b"


def test_expired_night_keys():
    dates = {"2026-08-01": date(2026, 8, 1), "2026-08-10": date(2026, 8, 10)}
    assert expired_night_keys(dates, today=date(2026, 8, 5)) == ["2026-08-01"]
