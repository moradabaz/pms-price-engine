from collections.abc import Sequence
from dataclasses import dataclass

from pricing_formulas.decision_components import DecisionComponent
from pricing_formulas.layers.booking_window import (
    FloorPolicy,
    FloorType,
    booking_window_floor,
)
from pricing_formulas.layers.guardrails import RuleApplied, apply_guardrails
from pricing_formulas.layers.inventory import inventory_layer
from pricing_formulas.layers.market import market_reference_price
from pricing_formulas.layers.performance import performance_layer
from pricing_formulas.layers.structural import property_reference_price

# Phase 9 (ADR-0011 backlog #1, docs/phase-9-los-floor-matrix-design-decisions.md
# §C): candidate stay lengths for the LOS floor matrix.
LOS_CANDIDATES: tuple[int, ...] = (1, 2, 3, 7, 14)


@dataclass(frozen=True)
class PriceCalculation:
    minimum_price_eur: float
    floor_type: FloorType
    floor_policy: FloorPolicy
    property_attribute_factor: float
    property_reference_price_eur: float
    market_reference_price_eur: float
    rule_applied: RuleApplied
    suggested_price_eur: float
    below_market_by: float
    effective_margin: float
    decision_components: list[DecisionComponent]


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
    commission_netting_eur: float = 0.0,
    property_decision_components: Sequence[DecisionComponent] = (),
    commission_decision_components: Sequence[DecisionComponent] = (),
) -> PriceCalculation:
    """Orchestrates the layered Revenue Management engine (ADR-0011 backlog
    #7, spec 13): Structural -> Performance -> Inventory -> Market ->
    Booking-Window -> Guardrails, with Commercial's netting pre-computed by
    the caller (Stage B resolves it once per decision before calling this).
    Structural's own attribute factor is likewise pre-resolved by the
    caller — it's computed once per apartment per segment-broadcast update
    in Stage A, not per decision; only its per-decision half
    (property_reference_price) runs inside this pipeline. Returns a
    PriceCalculation."""
    # Phase 9: fixed_cost_eur/variable_cost_eur are already per-night rates
    # (cost_aggregation.py divides by available_days) — they never scale with
    # stay_length. one_time_cost_eur (Cr) is a lump sum per booking and is the
    # only term amortized across the stay. For stay_length=1 this is
    # algebraically identical to the pre-Phase-9 formula.
    one_time_cost_per_night_eur = one_time_cost_eur / stay_length

    booking_window = booking_window_floor(
        days_to_arrival,
        fixed_cost_eur,
        variable_cost_eur,
        one_time_cost_per_night_eur,
        target_margin,
        commission_pct,
        commission_netting_eur,
    )

    # ADR-0011 (backlog #6): the segment's raw market average, adjusted for
    # this specific apartment's Bonus/Malus attributes — apartments in the
    # same segment no longer share an identical competitive threshold.
    property_reference_price_eur = property_reference_price(
        avg_nightly_rate_eur, property_attribute_factor
    )
    # Phase 13 (ADR-0011 backlog #7): Performance/Inventory stubs, always
    # neutral until backlog #11 — multiplying by 1.0 is a no-op today,
    # becomes real signal once that phase lands real market/comp-set data.
    property_reference_price_eur *= performance_layer() * inventory_layer()

    market_reference_price_eur = market_reference_price(
        property_reference_price_eur, competitiveness_discount
    )

    guardrails = apply_guardrails(
        booking_window.minimum_price_eur,
        market_reference_price_eur,
        property_reference_price_eur,
    )

    total_cost_eur = fixed_cost_eur + variable_cost_eur + one_time_cost_per_night_eur
    effective_margin = (
        (guardrails.suggested_price_eur / total_cost_eur) - 1 if total_cost_eur else 0.0
    )

    # Phase 10/11 (ADR-0011 backlog #4/#5): property_decision_components and
    # commission_decision_components are both empty for LOS-matrix candidates
    # (decide_price_los_matrix() never forwards either), so their
    # decision_components naturally ends up as just [rule_component] — no
    # second code path (spec 10 §E, spec 11 §F/AC-06). commission_netting_eur
    # itself (the numeric floor adjustment) IS still forwarded to every
    # candidate — only the component that explains it is top-level-only.
    decision_components = [
        *property_decision_components,
        *commission_decision_components,
        guardrails.rule_component,
    ]

    return PriceCalculation(
        minimum_price_eur=round(booking_window.minimum_price_eur, 2),
        floor_type=booking_window.floor_type,
        floor_policy=booking_window.floor_policy,
        property_attribute_factor=property_attribute_factor,
        property_reference_price_eur=round(property_reference_price_eur, 2),
        market_reference_price_eur=round(market_reference_price_eur, 2),
        rule_applied=guardrails.rule_applied,
        suggested_price_eur=round(guardrails.suggested_price_eur, 2),
        below_market_by=round(guardrails.below_market_by, 2),
        effective_margin=round(effective_margin, 4),
        decision_components=decision_components,
    )


@dataclass(frozen=True)
class LosFloorCandidate:
    stay_length: int
    minimum_price_eur: float
    floor_type: FloorType
    floor_policy: FloorPolicy
    rule_applied: RuleApplied
    suggested_price_eur: float
    effective_margin: float
    decision_components: list[DecisionComponent]


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
    commission_netting_eur: float = 0.0,
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
            commission_netting_eur=commission_netting_eur,
        )
        candidates.append(
            LosFloorCandidate(
                stay_length=stay_length,
                minimum_price_eur=calc.minimum_price_eur,
                floor_type=calc.floor_type,
                floor_policy=calc.floor_policy,
                rule_applied=calc.rule_applied,
                suggested_price_eur=calc.suggested_price_eur,
                effective_margin=calc.effective_margin,
                decision_components=calc.decision_components,
            )
        )
    return candidates
