from typing import cast

from pricing_formulas.decision_components import DecisionComponent, RuleReasonCode

# Phase 22 (ADR-0015, spec 22 §3): the external spec's layer D (Booking
# Window — early bird / standard window / last minute / same day, §14.1).
# Tiers as (min_days_inclusive, adjustment) pairs, ordered furthest-out
# first. Positive = markup, negative = markdown. Configurable, not embedded
# irreversibly — same discipline QUALITY_TIER_ADJUSTMENTS (structural.py)
# already established. Placeholder values, not sourced from a real BiLemon
# policy (spec 22 §6 known limitation).
BOOKING_WINDOW_TIERS: tuple[tuple[int, float, str], ...] = (
    (45, 0.00, "standard_window"),  # >= 45 days: no adjustment (baseline)
    (15, -0.03, "early_bird"),  # 15-44 days: small markdown, reward early commitment
    (3, 0.00, "standard_window"),  # 3-14 days: no adjustment
    (1, -0.05, "last_minute"),  # 1-2 days: markdown to move unsold inventory
    (0, -0.08, "same_day"),  # 0 days: steepest markdown
)


def booking_window_factor(days_to_arrival: int) -> float:
    """Returns the multiplicative factor for this booking window tier.
    Clamped at 0 days minimum (a negative days_to_arrival — a decision built
    for a past date — is not expected to reach here; Stage B already skips
    market updates for past target_dates)."""
    days = max(days_to_arrival, 0)
    for threshold, adjustment, _label in BOOKING_WINDOW_TIERS:
        if days >= threshold:
            return round(1 + adjustment, 4)
    return 1.0  # unreachable given the 0-days final tier, kept for safety


def booking_window_component(days_to_arrival: int) -> DecisionComponent | None:
    """Explains the booking-window adjustment. None when the tier's own
    adjustment is 0.0 (standard_window) — same 'don't emit a component that
    explains nothing' rule commercial.py's netting component already
    follows."""
    days = max(days_to_arrival, 0)
    for threshold, adjustment, label in BOOKING_WINDOW_TIERS:
        if days >= threshold:
            if adjustment == 0.0:
                return None
            return DecisionComponent(
                code=cast(RuleReasonCode, f"rule_{label}"),
                label=(
                    f"Booking window: {days} days to arrival "
                    f"({label}, {adjustment:+.0%})"
                ),
                impact=adjustment,
            )
    return None
