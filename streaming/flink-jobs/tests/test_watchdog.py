from flink_jobs.watchdog import expired_keys


def test_exact_match_is_expired():
    deadlines = {"apt-A": 1000, "apt-B": 2000}
    assert expired_keys(deadlines, 1000) == ["apt-A"]


def test_no_match_returns_empty():
    deadlines = {"apt-A": 2000}
    assert expired_keys(deadlines, 1000) == []


def test_superseded_deadline_is_not_expired():
    # apt-A's deadline moved after a newer update — the old timer firing
    # for the earlier timestamp must not match anymore.
    deadlines = {"apt-A": 3000}
    assert expired_keys(deadlines, 1000) == []
