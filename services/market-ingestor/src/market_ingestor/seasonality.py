from datetime import date

# Confirmed 2026-07-28 (Decision D.2, docs/phase-4-streaming-design-decisions.md):
# seasonality lives here, in market-ingestor, not in Phase 4's Flink job — Phase 4
# always reads avg_nightly_rate_eur(target_date) as-is, already "cooked" with
# seasonality baked in. Same table for all 3 cities in this first version
# (Barcelona/Madrid/Valencia) — a per-city table is a future refinement if the
# researched sources (spec §5.2) turn out to disagree enough to justify it.
_HIGH_SEASON_MONTHS = {7, 8}  # verano — the peak explicitly named in Punto 4
_SHOULDER_SEASON_MONTHS = {5, 6, 9, 10}

HIGH_SEASON_MULTIPLIER = 1.30
SHOULDER_SEASON_MULTIPLIER = 1.05
LOW_SEASON_MULTIPLIER = 0.85  # invierno (Nov-Apr) — the trough explicitly named in Punto 4


def seasonal_multiplier(target_date: date) -> float:
    """Multiplier applied to a segment's reference median price before the
    log-normal sampling step (pricing.py), keyed only by month — deliberately
    not city-specific yet (D.2)."""
    if target_date.month in _HIGH_SEASON_MONTHS:
        return HIGH_SEASON_MULTIPLIER
    if target_date.month in _SHOULDER_SEASON_MONTHS:
        return SHOULDER_SEASON_MULTIPLIER
    return LOW_SEASON_MULTIPLIER
