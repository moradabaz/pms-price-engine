from typing import Literal

from pricing_formulas.decision_components import DecisionComponent

# Phase 11 (ADR-0011 backlog #5, spec 11 §4).
CommissionBase = Literal[
    "total_revenue", "revenue_minus_ota", "revenue_minus_ota_minus_cleaning"
]


def netted_commission_amount(
    commission_base: CommissionBase,
    ota_related_cost_eur: float,
    cleaning_cost_eur: float,
) -> float:
    """The per-night amount netted out of the commission base (ADR-0011
    backlog #5, spec 11 §4) — 0 for total_revenue. Both inputs stay fully
    counted inside fixed_cost_eur/variable_cost_eur (cost_aggregation.py);
    this is purely which amount commission_pct is *not* charged against.
    Returns the netted amount."""
    if commission_base == "total_revenue":
        return 0.0
    if commission_base == "revenue_minus_ota":
        return ota_related_cost_eur
    return ota_related_cost_eur + cleaning_cost_eur


def commission_base_netting_component(
    commission_base: CommissionBase, commission_pct: float, net: float
) -> DecisionComponent | None:
    """Explains the floor reduction from a netted commission base — None
    when there's nothing to net (total_revenue, or a netting base whose
    underlying amount happens to be zero this period), matching the
    "don't emit a component that explains nothing" principle (spec 11 §F).
    Returns the component, or None."""
    if net <= 0:
        return None
    impact = -round(commission_pct * net, 2)
    return DecisionComponent(
        code="commission_base_netting",
        label=(
            f"Commission base '{commission_base}' nets {net} EUR/night, "
            f"reducing the floor by {-impact} EUR"
        ),
        impact=impact,
    )
