from dataclasses import dataclass
from typing import Literal

RuleApplied = Literal["market_competitive", "minimum_floor", "cost_protected"]


@dataclass(frozen=True)
class PriceCalculation:
    minimum_price_eur: float
    market_reference_price_eur: float
    rule_applied: RuleApplied
    suggested_price_eur: float
    below_market_by: float
    effective_margin: float


def decide_price(
    daily_cost_eur: float,
    target_margin: float,
    avg_nightly_rate_eur: float,
    competitiveness_discount: float,
) -> PriceCalculation:
    """Computes the suggested nightly price and which rule applied.
    Returns a PriceCalculation."""
    minimum_price_eur = daily_cost_eur * (1 + target_margin)
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
    effective_margin = (
        (suggested_price_eur / daily_cost_eur) - 1 if daily_cost_eur else 0.0
    )

    return PriceCalculation(
        minimum_price_eur=round(minimum_price_eur, 2),
        market_reference_price_eur=round(market_reference_price_eur, 2),
        rule_applied=rule_applied,
        suggested_price_eur=round(suggested_price_eur, 2),
        below_market_by=round(below_market_by, 2),
        effective_margin=round(effective_margin, 4),
    )
