# Phase 8 (docs/adr/ADR-0011 backlog #6,
# docs/phase-8-property-bonus-malus-design-decisions.md §B): weights as named
# constants, not inline arithmetic — "configurable, not embedded irreversibly
# in code" is the source spec's own stated principle.
# Additive-percentage combination: each attribute contributes a bounded
# +/-% adjustment, summed into a single multiplier, then clamped.
QUALITY_TIER_ADJUSTMENTS: dict[str, float] = {
    "basic": -0.15,
    "standard": 0.00,
    "premium": 0.15,
    "luxury": 0.30,
}
RATING_BASELINE = 4.0
RATING_WEIGHT = 0.10
VIEW_ADJUSTMENT = 0.05
PARKING_ADJUSTMENT = 0.04
MIN_FACTOR = 0.5
MAX_FACTOR = 2.0


def property_attribute_factor(
    quality_tier: str, rating: float, has_view: bool, has_parking: bool
) -> float:
    """Combines an apartment's Bonus/Malus attributes into a single
    multiplier for its market reference price (ADR-0011). Returns the
    clamped factor."""
    adjustment = QUALITY_TIER_ADJUSTMENTS[quality_tier]
    adjustment += (rating - RATING_BASELINE) * RATING_WEIGHT
    adjustment += VIEW_ADJUSTMENT if has_view else 0.0
    adjustment += PARKING_ADJUSTMENT if has_parking else 0.0
    return round(min(max(1 + adjustment, MIN_FACTOR), MAX_FACTOR), 4)
