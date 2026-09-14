from typing import Literal

from pricing_formulas.layers.guardrails import RuleApplied

# Phase 23 (ADR-0016, spec 23 §3): the external spec's §13 situation/action
# table as an explicit classification, evaluated once per decision.
ViabilityStatus = Literal[
    "ok",
    "demand_upside",
    "min_stay_lever_available",
    "channel_lever_available",
    "persistent_floor_breach",
    "override_active",
    "floor_binding",
]

# Phase 23 (ADR-0016 §3): matches client spec test scenario T08 literally.
# Configurable, not hard-coded (client spec §3).
PERSISTENT_BREACH_THRESHOLD_DAYS = 30


def classify_viability(
    rule_applied: RuleApplied,
    below_market_by: float,
    recommended_min_stay: int | None,
    any_channel_market_competitive: bool,
    manual_override_active: bool,
    floor_breach_days: int,
) -> ViabilityStatus:
    """Pure priority-ordered classification (external spec §13): an active
    override always wins (a human has already made the call), then a
    persistent structural problem, then the levers, then ok/upside/binding.

    Takes primitives rather than the MinimumStayRecommendation/
    ChannelPriceCandidate dataclasses those values are drawn from (spec 23
    §3's own pseudocode passes the whole objects) — this module lives in
    libs/pricing-formulas and both those types are defined in engine.py,
    which needs to import this module's ViabilityStatus in turn; passing
    only the two primitives this function actually reads avoids a circular
    import between engine.py and viability.py without losing anything the
    classification needs.

    `recommended_min_stay` must be strictly greater than 1 to count as a
    genuine lever — recommend_minimum_stay() also returns
    recommended_min_stay=1 (not None) whenever the 1-night candidate is
    already outside minimum_profitable_price (e.g. rule_applied ==
    "minimum_floor"), which is not a lever to reach for, just the
    already-current stay length. Spec 23 §3's own pseudocode checked only
    `is not None`, which would misclassify that minimum_floor case as
    min_stay_lever_available — corrected here (see spec 23 §7).

    `manual_override_active` is always False when called from Stage B
    (stage_price_decision.py) — override application happens in Stage C,
    after this classification already ran. Stage C's own
    stage_manual_override_enrichment.py overwrites viability_status to
    "override_active" directly when it actually applies an override,
    rather than this function ever seeing override state itself (spec 23
    §7 — decide_price() has no override input at all, Stage C runs
    strictly after Stage B).

    Returns the single highest-priority status that applies."""
    if manual_override_active:
        return "override_active"
    if floor_breach_days >= PERSISTENT_BREACH_THRESHOLD_DAYS:
        return "persistent_floor_breach"
    if rule_applied == "market_competitive":
        return "demand_upside" if below_market_by <= 0 else "ok"
    if recommended_min_stay is not None and recommended_min_stay > 1:
        return "min_stay_lever_available"
    if any_channel_market_competitive:
        return "channel_lever_available"
    return "floor_binding"
