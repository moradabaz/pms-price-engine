from datetime import datetime


def is_safe_to_overwrite(
    candidate_updated_at: datetime, existing_updated_at: datetime | None
) -> bool:
    """Checks candidate_updated_at against existing_updated_at. Returns
    True if safe to overwrite, False if it's a stale replay to discard."""
    if existing_updated_at is None:
        return True
    return candidate_updated_at >= existing_updated_at
