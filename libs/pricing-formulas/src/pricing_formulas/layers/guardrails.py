from dataclasses import dataclass
from typing import Literal

from pricing_formulas.decision_components import DecisionComponent

RuleApplied = Literal["market_competitive", "minimum_floor", "cost_protected"]


def rule_decision_component(
    rule_applied: RuleApplied,
    minimum_price_eur: float,
    market_reference_price_eur: float,
    property_reference_price_eur: float,
) -> DecisionComponent:
    """Explains which pricing rule fired and by how much (Phase 10, ADR-0011
    backlog #4) — impact is the signed EUR gap that decided the branch.
    Returns the component."""
    if rule_applied == "market_competitive":
        impact = round(market_reference_price_eur - minimum_price_eur, 2)
        return DecisionComponent(
            code="rule_market_competitive",
            label=(
                f"Market reference price ({market_reference_price_eur}) "
                f"clears the cost floor ({minimum_price_eur}) by {impact} EUR"
            ),
            impact=impact,
        )
    if rule_applied == "minimum_floor":
        impact = round(minimum_price_eur - market_reference_price_eur, 2)
        return DecisionComponent(
            code="rule_minimum_floor",
            label=(
                f"Cost floor ({minimum_price_eur}) exceeds market reference "
                f"({market_reference_price_eur}) by {impact} EUR but stays "
                f"within property reference ({property_reference_price_eur})"
            ),
            impact=impact,
        )
    impact = round(minimum_price_eur - property_reference_price_eur, 2)
    return DecisionComponent(
        code="rule_cost_protected",
        label=(
            f"Cost floor ({minimum_price_eur}) exceeds property reference "
            f"price ({property_reference_price_eur}) by {impact} EUR"
        ),
        impact=impact,
    )


@dataclass(frozen=True)
class GuardrailsResult:
    rule_applied: RuleApplied
    suggested_price_eur: float
    below_market_by: float
    rule_component: DecisionComponent


def apply_guardrails(
    minimum_price_eur: float,
    market_reference_price_eur: float,
    property_reference_price_eur: float,
) -> GuardrailsResult:
    """Guardrails layer (ADR-0011 backlog #7, spec 13 §3/§C) — the final
    floor-vs-market arbitration: "the system publishes max(market, floor)"
    (docs/AUDIT_DIARY.md:119). Pure extraction of engine.py's former final
    if/elif/else, unchanged. IMPORTANT: the comparisons happen on UNROUNDED
    inputs, exactly as before — only rule_decision_component()'s label
    arguments are rounded. Changing this order would silently shift
    rule_applied at boundary cases (spec 13 §F). Returns the arbitration
    result."""
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
    rule_component = rule_decision_component(
        rule_applied,
        round(minimum_price_eur, 2),
        round(market_reference_price_eur, 2),
        round(property_reference_price_eur, 2),
    )
    return GuardrailsResult(
        rule_applied=rule_applied,
        suggested_price_eur=suggested_price_eur,
        below_market_by=below_market_by,
        rule_component=rule_component,
    )
