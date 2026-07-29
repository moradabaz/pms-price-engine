from datetime import UTC, datetime

from flink_jobs.staleness import is_safe_to_overwrite


def test_no_existing_value_is_always_safe():
    assert is_safe_to_overwrite(datetime(2026, 1, 1, tzinfo=UTC), None) is True


def test_newer_candidate_is_safe():
    existing = datetime(2026, 1, 1, tzinfo=UTC)
    candidate = datetime(2026, 1, 2, tzinfo=UTC)
    assert is_safe_to_overwrite(candidate, existing) is True


def test_older_candidate_is_rejected():
    existing = datetime(2026, 1, 2, tzinfo=UTC)
    candidate = datetime(2026, 1, 1, tzinfo=UTC)
    assert is_safe_to_overwrite(candidate, existing) is False


def test_equal_timestamps_are_safe():
    same = datetime(2026, 1, 1, tzinfo=UTC)
    assert is_safe_to_overwrite(same, same) is True
