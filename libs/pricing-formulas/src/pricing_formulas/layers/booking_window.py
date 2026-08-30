from dataclasses import dataclass
from typing import Literal

FloorType = Literal[
    "structural_full_margin", "structural_reduced_margin", "contribution"
]
# Phase 12 (ADR-0011 backlog #10, spec 12 §3): explicit classification of
# floor_type into the external spec's Hard/Soft floor vocabulary.
FloorPolicy = Literal["hard", "soft"]

# ADR-0009 (D4): antelación tier boundaries and the margin cut in the 15-30
# day band, taken from the stakeholders' own table.
STRUCTURAL_FULL_MARGIN_THRESHOLD_DAYS = 30
STRUCTURAL_REDUCED_MARGIN_THRESHOLD_DAYS = 15
REDUCED_MARGIN_FACTOR = 0.75


def floor_policy_for(floor_type: FloorType) -> FloorPolicy:
    """Classifies a floor_type as the external spec's Hard floor (absolute,
    never crossed) or Soft floor (a margin target, relaxable by antelación —
    ADR-0011 backlog #10, spec 12 §4). Returns the policy."""
    return "hard" if floor_type == "contribution" else "soft"


@dataclass(frozen=True)
class BookingWindowResult:
    floor_type: FloorType
    floor_policy: FloorPolicy
    minimum_price_eur: float


def booking_window_floor(
    days_to_arrival: int,
    fixed_cost_eur: float,
    variable_cost_eur: float,
    one_time_cost_per_night_eur: float,
    target_margin: float,
    commission_pct: float,
    commission_netting_eur: float,
) -> BookingWindowResult:
    """Booking-Window layer (ADR-0011 backlog #7, spec 13 §3) — selects the
    antelación-tiered floor formula (ADR-0009 D4) and computes
    minimum_price_eur. Pure extraction of engine.py's former if/elif/else,
    unchanged formula. Phase 11 (ADR-0011 backlog #5): charging
    commission_pct against a netted base instead of the full suggested price
    nets commission_netting_eur out of every floor tier's numerator (spec 11
    §4 derivation); 0.0 reproduces every pre-Phase-11 formula exactly.
    Returns the unrounded floor, its type, and its policy."""
    if days_to_arrival > STRUCTURAL_FULL_MARGIN_THRESHOLD_DAYS:
        floor_type: FloorType = "structural_full_margin"
        minimum_price_eur = (
            fixed_cost_eur
            + variable_cost_eur
            + one_time_cost_per_night_eur
            - commission_netting_eur
        ) / (1 - target_margin - commission_pct)
    elif days_to_arrival >= STRUCTURAL_REDUCED_MARGIN_THRESHOLD_DAYS:
        floor_type = "structural_reduced_margin"
        reduced_margin = target_margin * REDUCED_MARGIN_FACTOR
        minimum_price_eur = (
            fixed_cost_eur
            + variable_cost_eur
            + one_time_cost_per_night_eur
            - commission_netting_eur
        ) / (1 - reduced_margin - commission_pct)
    else:
        # Contribution floor (7-14d and 0-3d alike, ADR-0009): Cf excluded —
        # it's sunk whether or not this booking happens. No margin term.
        floor_type = "contribution"
        minimum_price_eur = (
            variable_cost_eur + one_time_cost_per_night_eur - commission_netting_eur
        ) / (1 - commission_pct)

    return BookingWindowResult(
        floor_type=floor_type,
        floor_policy=floor_policy_for(floor_type),
        minimum_price_eur=minimum_price_eur,
    )
