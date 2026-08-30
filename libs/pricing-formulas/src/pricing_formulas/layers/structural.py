from pricing_formulas.decision_components import DecisionComponent

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
# Phase 13 (ADR-0011 backlog #7, spec 13 §C): this clamp is an internal bound
# on Structural's own arithmetic, not a cross-layer arbitration — it stays
# here rather than moving to guardrails.py. See spec 13 §C for the reasoning.
MIN_FACTOR = 0.5
MAX_FACTOR = 2.0


def property_attribute_components(
    quality_tier: str, rating: float, has_view: bool, has_parking: bool
) -> list[DecisionComponent]:
    """Breaks an apartment's Bonus/Malus attributes into their four
    independent contributions (Phase 10, ADR-0011 backlog #4). All four are
    always returned, even at impact=0.0 — "evaluated, no effect" is still
    informative (spec 10 §3). Returns the four components."""
    return [
        DecisionComponent(
            code="property_quality_tier",
            label=(
                f"Quality tier '{quality_tier}' "
                f"({QUALITY_TIER_ADJUSTMENTS[quality_tier]:+.0%})"
            ),
            impact=QUALITY_TIER_ADJUSTMENTS[quality_tier],
        ),
        DecisionComponent(
            code="property_rating",
            label=f"Rating {rating} vs baseline {RATING_BASELINE}",
            impact=round((rating - RATING_BASELINE) * RATING_WEIGHT, 4),
        ),
        DecisionComponent(
            code="property_view",
            label="Has view" if has_view else "No view",
            impact=VIEW_ADJUSTMENT if has_view else 0.0,
        ),
        DecisionComponent(
            code="property_parking",
            label="Has parking" if has_parking else "No parking",
            impact=PARKING_ADJUSTMENT if has_parking else 0.0,
        ),
    ]


def property_attribute_factor(
    quality_tier: str, rating: float, has_view: bool, has_parking: bool
) -> float:
    """Combines an apartment's Bonus/Malus attributes into a single
    multiplier for its market reference price (ADR-0011). Returns the
    clamped factor."""
    adjustment = sum(
        component.impact
        for component in property_attribute_components(
            quality_tier, rating, has_view, has_parking
        )
    )
    return round(min(max(1 + adjustment, MIN_FACTOR), MAX_FACTOR), 4)


def property_reference_price(
    avg_nightly_rate_eur: float, property_attribute_factor: float
) -> float:
    """Structural layer's per-decision output (ADR-0011 backlog #7, spec 13
    §3) — the segment's raw market average, adjusted for this specific
    apartment's Bonus/Malus attributes. Pure extraction of engine.py's
    former inline multiplication; unrounded, rounded once by the caller.
    Returns the property reference price."""
    return avg_nightly_rate_eur * property_attribute_factor
