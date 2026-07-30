from datetime import datetime

FRESHNESS_THRESHOLD_MS = 48 * 60 * 60 * 1000


def next_deadline_millis(now: datetime) -> int:
    """Computes the epoch-millis deadline 48h from now. Returns the int timestamp."""
    return int(now.timestamp() * 1000) + FRESHNESS_THRESHOLD_MS


def expired_keys(deadlines: dict[str, int], fired_at_millis: int) -> list[str]:
    """Finds sub-keys whose stored deadline (epoch millis) equals fired_at_millis.
    Returns the list of expired keys."""
    return [key for key, deadline in deadlines.items() if deadline == fired_at_millis]
