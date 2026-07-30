from collections.abc import Mapping
from datetime import date, datetime
from typing import Protocol, TypeVar

HOJA1_MAX_ENTRIES_PER_SEGMENT = 500


class HasUpdatedAt(Protocol):
    updated_at: datetime


K = TypeVar("K")
V = TypeVar("V", bound=HasUpdatedAt)


def is_over_capacity(
    current_size: int, cap: int = HOJA1_MAX_ENTRIES_PER_SEGMENT
) -> bool:
    """Checks a MapState's size against a cap. Returns True if over cap."""
    return current_size > cap


def oldest_key_by_updated_at(entries: Mapping[K, V]) -> K:
    """Finds the entry with the smallest updated_at. Returns its key."""
    return min(entries, key=lambda key: entries[key].updated_at)


def expired_night_keys(target_dates: Mapping[K, date], today: date) -> list[K]:
    """Finds entries whose target_date is before today. Returns their keys."""
    return [key for key, target_date in target_dates.items() if target_date < today]
