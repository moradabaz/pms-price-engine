from dataclasses import dataclass
from typing import Literal

RuleApplied = Literal["market_competitive", "minimum_floor", "cost_protected"]
FloorType = Literal[
    "structural_full_margin", "structural_reduced_margin", "contribution"
]

# ADR-0009 (D4): antelación tier boundaries and the margin cut in the 15-30
# day band, taken from the stakeholders' own table.
STRUCTURAL_FULL_MARGIN_THRESHOLD_DAYS = 30
STRUCTURAL_REDUCED_MARGIN_THRESHOLD_DAYS = 15
REDUCED_MARGIN_FACTOR = 0.75

# Phase 9 (ADR-0011 backlog #1, docs/phase-9-los-floor-matrix-design-decisions.md
# §C): candidate stay lengths for the LOS floor matrix.
LOS_CANDIDATES: tuple[int, ...] = (1, 2, 3, 7, 14)


@dataclass(frozen=True)
class PriceCalculation:
    minimum_price_eur: float
    floor_type: FloorType
    property_attribute_factor: float
    property_reference_price_eur: float
    market_reference_price_eur: float
    rule_applied: RuleApplied
    suggested_price_eur: float
    below_market_by: float
    effective_margin: float


def decide_price(
    fixed_cost_eur: float,
    variable_cost_eur: float,
    one_time_cost_eur: float,
    target_margin: float,
    commission_pct: float,
    avg_nightly_rate_eur: float,
    competitiveness_discount: float,
    days_to_arrival: int,
    property_attribute_factor: float = 1.0,
    stay_length: int = 1,
) -> PriceCalculation:
    """Computes the suggested nightly price and which rule/floor applied
    (ADR-0009, ADR-0011 backlog #6/#1). Returns a PriceCalculation."""
    # Phase 9: fixed_cost_eur/variable_cost_eur are already per-night rates
    # (cost_aggregation.py divides by available_days) — they never scale with
    # stay_length. one_time_cost_eur (Cr) is a lump sum per booking and is the
    # only term amortized across the stay. For stay_length=1 this is
    # algebraically identical to the pre-Phase-9 formula.
    one_time_cost_per_night_eur = one_time_cost_eur / stay_length

    if days_to_arrival > STRUCTURAL_FULL_MARGIN_THRESHOLD_DAYS:
        floor_type: FloorType = "structural_full_margin"
        minimum_price_eur = (
            fixed_cost_eur + variable_cost_eur + one_time_cost_per_night_eur
        ) / (1 - target_margin - commission_pct)
    elif days_to_arrival >= STRUCTURAL_REDUCED_MARGIN_THRESHOLD_DAYS:
        floor_type = "structural_reduced_margin"
        reduced_margin = target_margin * REDUCED_MARGIN_FACTOR
        minimum_price_eur = (
            fixed_cost_eur + variable_cost_eur + one_time_cost_per_night_eur
        ) / (1 - reduced_margin - commission_pct)
    else:
        # Contribution floor (7-14d and 0-3d alike, ADR-0009): Cf excluded —
        # it's sunk whether or not this booking happens. No margin term.
        floor_type = "contribution"
        minimum_price_eur = (variable_cost_eur + one_time_cost_per_night_eur) / (
            1 - commission_pct
        )

    # ADR-0011 (backlog #6): the segment's raw market average, adjusted for
    # this specific apartment's Bonus/Malus attributes — apartments in the
    # same segment no longer share an identical competitive threshold.
    property_reference_price_eur = avg_nightly_rate_eur * property_attribute_factor
    market_reference_price_eur = property_reference_price_eur * (
        1 - competitiveness_discount
    )

    rule_applied: RuleApplied
    if minimum_price_eur <= market_reference_price_eur:
        rule_applied = "market_competitive"
        suggested_price_eur = market_reference_price_eur
    elif minimum_price_eur <= property_reference_price_eur:
        rule_applied = "minimum_floor"
        suggested_price_eur = minimum_price_eur
    else:
        rule_applied = "cost_protected"
        suggested_price_eur = minimum_price_eur

    below_market_by = property_reference_price_eur - suggested_price_eur
    total_cost_eur = fixed_cost_eur + variable_cost_eur + one_time_cost_per_night_eur
    effective_margin = (
        (suggested_price_eur / total_cost_eur) - 1 if total_cost_eur else 0.0
    )

    return PriceCalculation(
        minimum_price_eur=round(minimum_price_eur, 2),
        floor_type=floor_type,
        property_attribute_factor=property_attribute_factor,
        property_reference_price_eur=round(property_reference_price_eur, 2),
        market_reference_price_eur=round(market_reference_price_eur, 2),
        rule_applied=rule_applied,
        suggested_price_eur=round(suggested_price_eur, 2),
        below_market_by=round(below_market_by, 2),
        effective_margin=round(effective_margin, 4),
    )


@dataclass(frozen=True)
class LosFloorCandidate:
    stay_length: int
    minimum_price_eur: float
    floor_type: FloorType
    rule_applied: RuleApplied
    suggested_price_eur: float
    effective_margin: float


def decide_price_los_matrix(
    fixed_cost_eur: float,
    variable_cost_eur: float,
    one_time_cost_eur: float,
    target_margin: float,
    commission_pct: float,
    avg_nightly_rate_eur: float,
    competitiveness_discount: float,
    days_to_arrival: int,
    property_attribute_factor: float = 1.0,
    stay_lengths: tuple[int, ...] = LOS_CANDIDATES,
) -> list[LosFloorCandidate]:
    """Evaluates decide_price() once per candidate stay length (ADR-0011
    backlog #1). No formula duplicated — a thin composition over
    decide_price(), since only minimum_price_eur/rule_applied/
    suggested_price_eur/effective_margin vary with stay_length; the market
    side does not (docs/phase-9-los-floor-matrix-design-decisions.md §A).
    Returns one LosFloorCandidate per stay length."""
    candidates = []
    for stay_length in stay_lengths:
        calc = decide_price(
            fixed_cost_eur=fixed_cost_eur,
            variable_cost_eur=variable_cost_eur,
            one_time_cost_eur=one_time_cost_eur,
            target_margin=target_margin,
            commission_pct=commission_pct,
            avg_nightly_rate_eur=avg_nightly_rate_eur,
            competitiveness_discount=competitiveness_discount,
            days_to_arrival=days_to_arrival,
            property_attribute_factor=property_attribute_factor,
            stay_length=stay_length,
        )
        candidates.append(
            LosFloorCandidate(
                stay_length=stay_length,
                minimum_price_eur=calc.minimum_price_eur,
                floor_type=calc.floor_type,
                rule_applied=calc.rule_applied,
                suggested_price_eur=calc.suggested_price_eur,
                effective_margin=calc.effective_margin,
            )
        )
    return candidates
