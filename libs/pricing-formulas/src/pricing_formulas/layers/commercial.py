from typing import Literal

from pricing_formulas.decision_components import DecisionComponent

# Phase 11 (ADR-0011 backlog #5, spec 11 §4). Phase 20 (ADR-0013 §3):
# generalized from commission-only to any percentage CostDefinition
# (calculation_base='pct_adjusted_revenue') that declares a revenue_base —
# the values are unchanged, only the name (it's no longer commission-only).
# Phase 24 (ADR-0017 §1): the remaining 2 of the external spec's 5 bases
# (§11.1) added — all five are linear subtractions from Total Revenue with
# no interdependent unknowns, so a closed Literal + one branch per value
# stays fully enumerable/testable; a general symbolic/solver-based engine
# (spec §29's own open question) is explicitly deferred.
RevenueBase = Literal[
    "total_revenue",
    "revenue_minus_ota",
    "revenue_minus_ota_minus_cleaning",
    "revenue_minus_ota_minus_cleaning_minus_laundry",
    "revenue_minus_all_booking_costs",
]


def netted_revenue_base_amount(
    revenue_base: RevenueBase,
    ota_related_cost_eur: float,
    cleaning_cost_eur: float,
    laundry_cost_eur: float = 0.0,
    booking_scope_cost_eur: float = 0.0,
) -> float:
    """The per-night amount netted out of a percentage cost's revenue base
    (ADR-0011 backlog #5, generalized by ADR-0013 §3, extended by ADR-0017
    §1) — 0 for total_revenue. Every input stays fully counted inside
    fixed_cost_eur/variable_cost_eur/cost_breakdown (cost_aggregation.py);
    this is purely which amount a given rate is *not* charged against.
    "revenue_minus_all_booking_costs" nets booking_scope_cost_eur — the
    entire scope='booking' cost total, already the widest possible netting
    base for the period (by construction >= every other base's own netted
    amount for the same inputs, ADR-0017 §2's documented circularity if the
    percentage cost itself is also booking-scoped). Returns the netted
    amount."""
    if revenue_base == "total_revenue":
        return 0.0
    if revenue_base == "revenue_minus_ota":
        return ota_related_cost_eur
    if revenue_base == "revenue_minus_ota_minus_cleaning":
        return ota_related_cost_eur + cleaning_cost_eur
    if revenue_base == "revenue_minus_ota_minus_cleaning_minus_laundry":
        return ota_related_cost_eur + cleaning_cost_eur + laundry_cost_eur
    return booking_scope_cost_eur


def revenue_base_netting_component(
    revenue_base: RevenueBase, rate: float, net: float
) -> DecisionComponent | None:
    """Explains the floor reduction from a netted revenue base (ADR-0011
    backlog #5, generalized to any percentage CostDefinition by ADR-0013
    §3) — None when there's nothing to net (total_revenue, or a netting
    base whose underlying amount happens to be zero this period), matching
    the "don't emit a component that explains nothing" principle (spec 11
    §F). Returns the component, or None."""
    if net <= 0:
        return None
    impact = -round(rate * net, 2)
    return DecisionComponent(
        code="revenue_base_netting",
        label=(
            f"Revenue base '{revenue_base}' nets {net} EUR/night, "
            f"reducing the floor by {-impact} EUR"
        ),
        impact=impact,
    )
