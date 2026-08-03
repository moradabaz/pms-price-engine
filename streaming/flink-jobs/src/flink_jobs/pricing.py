from dataclasses import dataclass
from typing import Literal

RuleApplied = Literal["market_competitive", "minimum_floor", "cost_protected"]
FloorType = Literal[
    "structural_full_margin", "structural_reduced_margin", "contribution"
]

# ADR-0009 (D5): no per-stay-length modeling in this PoC — no reservation
# with an actual length exists anywhere upstream (Phases 1-3). Fixed at 1 is
# the conservative choice: it never under-covers the one-time cost (Cr)
# regardless of how long the real (unknown) stay turns out to be.
STAY_LENGTH_NIGHTS = 1

# ADR-0009 (D4): antelación tier boundaries and the margin cut in the 15-30
# day band, taken from the stakeholders' own table.
STRUCTURAL_FULL_MARGIN_THRESHOLD_DAYS = 30
STRUCTURAL_REDUCED_MARGIN_THRESHOLD_DAYS = 15
REDUCED_MARGIN_FACTOR = 0.75


@dataclass(frozen=True)
class PriceCalculation:
    minimum_price_eur: float
    floor_type: FloorType
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
) -> PriceCalculation:
    """Computes the suggested nightly price and which rule/floor applied
    (ADR-0009). Returns a PriceCalculation."""
    n = STAY_LENGTH_NIGHTS

    if days_to_arrival > STRUCTURAL_FULL_MARGIN_THRESHOLD_DAYS:
        floor_type: FloorType = "structural_full_margin"
        minimum_price_eur = (
            n * fixed_cost_eur + n * variable_cost_eur + one_time_cost_eur
        ) / (1 - target_margin - commission_pct)
    elif days_to_arrival >= STRUCTURAL_REDUCED_MARGIN_THRESHOLD_DAYS:
        floor_type = "structural_reduced_margin"
        reduced_margin = target_margin * REDUCED_MARGIN_FACTOR
        minimum_price_eur = (
            n * fixed_cost_eur + n * variable_cost_eur + one_time_cost_eur
        ) / (1 - reduced_margin - commission_pct)
    else:
        # Contribution floor (7-14d and 0-3d alike, ADR-0009): Cf excluded —
        # it's sunk whether or not this booking happens. No margin term.
        floor_type = "contribution"
        minimum_price_eur = (n * variable_cost_eur + one_time_cost_eur) / (
            1 - commission_pct
        )

    market_reference_price_eur = avg_nightly_rate_eur * (1 - competitiveness_discount)

    rule_applied: RuleApplied
    if minimum_price_eur <= market_reference_price_eur:
        rule_applied = "market_competitive"
        suggested_price_eur = market_reference_price_eur
    elif minimum_price_eur <= avg_nightly_rate_eur:
        rule_applied = "minimum_floor"
        suggested_price_eur = minimum_price_eur
    else:
        rule_applied = "cost_protected"
        suggested_price_eur = minimum_price_eur

    below_market_by = avg_nightly_rate_eur - suggested_price_eur
    total_cost_eur = n * fixed_cost_eur + n * variable_cost_eur + one_time_cost_eur
    effective_margin = (
        (suggested_price_eur / total_cost_eur) - 1 if total_cost_eur else 0.0
    )

    return PriceCalculation(
        minimum_price_eur=round(minimum_price_eur, 2),
        floor_type=floor_type,
        market_reference_price_eur=round(market_reference_price_eur, 2),
        rule_applied=rule_applied,
        suggested_price_eur=round(suggested_price_eur, 2),
        below_market_by=round(below_market_by, 2),
        effective_margin=round(effective_margin, 4),
    )
