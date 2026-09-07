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


@dataclass(frozen=True)
class MinimumStayRecommendation:
    recommended_min_stay: int | None
    floor_relief_eur: float | None
    cost_per_reservation_eur: float | None
    suggested_price_per_reservation_eur: float | None
    decision_component: DecisionComponent | None


def _reservation_cost_eur(
    stay_length: int,
    fixed_cost_eur: float,
    variable_cost_eur: float,
    one_time_cost_eur: float,
) -> float:
    """Total cost for a whole reservation of stay_length nights — the
    inverse of decide_price()'s per-night amortization (spec 15 §4-follow-up):
    fixed/variable costs recur every night, one_time_cost_eur is paid once
    per booking, not per night. Returns the rounded total."""
    return round(
        stay_length * (fixed_cost_eur + variable_cost_eur) + one_time_cost_eur, 2
    )


def recommend_minimum_stay(
    candidates: list[LosFloorCandidate],
    property_reference_price_eur: float,
    fixed_cost_eur: float,
    variable_cost_eur: float,
    one_time_cost_eur: float,
) -> MinimumStayRecommendation:
    """Minimum Stay as a profitability lever (ADR-0011 backlog #12, spec 15
    §4). Pure post-processing over an already-computed los_floor_matrix, plus
    the same raw cost inputs decide_price_los_matrix() itself received — no
    new formula duplicated. Relies on minimum_price_eur being monotonically
    non-increasing in stay_length (only one_time_cost_eur / n varies with n;
    floor_type/market_reference_price_eur/property_reference_price_eur are
    constant across candidates in one decision), so rule_applied can only
    move cost_protected -> minimum_floor -> market_competitive as n grows,
    never backwards — the shortest non-cost_protected candidate is therefore
    the unique correct threshold, not a heuristic. Alongside that stay
    length, also surfaces what the whole reservation would cost and what it
    should be priced at in total — a price already guaranteed to clear the
    cost floor and be at/below the market reference, since it's exactly the
    LOS candidate's own suggested_price_eur (never itself cost_protected)
    multiplied by the nights it covers. Returns the recommendation."""
    ordered = sorted(candidates, key=lambda c: c.stay_length)
    at_one_night = ordered[0]

    if at_one_night.rule_applied != "cost_protected":
        return MinimumStayRecommendation(
            recommended_min_stay=at_one_night.stay_length,
            floor_relief_eur=0.0,
            cost_per_reservation_eur=_reservation_cost_eur(
                at_one_night.stay_length,
                fixed_cost_eur,
                variable_cost_eur,
                one_time_cost_eur,
            ),
            suggested_price_per_reservation_eur=round(
                at_one_night.suggested_price_eur * at_one_night.stay_length, 2
            ),
            decision_component=None,
        )

    for candidate in ordered[1:]:
        if candidate.rule_applied != "cost_protected":
            floor_relief_eur = round(
                at_one_night.minimum_price_eur - candidate.minimum_price_eur, 2
            )
            suggested_price_per_reservation_eur = round(
                candidate.suggested_price_eur * candidate.stay_length, 2
            )
            return MinimumStayRecommendation(
                recommended_min_stay=candidate.stay_length,
                floor_relief_eur=floor_relief_eur,
                cost_per_reservation_eur=_reservation_cost_eur(
                    candidate.stay_length,
                    fixed_cost_eur,
                    variable_cost_eur,
                    one_time_cost_eur,
                ),
                suggested_price_per_reservation_eur=suggested_price_per_reservation_eur,
                decision_component=DecisionComponent(
                    code="minimum_stay_recommended",
                    label=(
                        f"At {at_one_night.stay_length} night(s), the cost "
                        f"floor ({at_one_night.minimum_price_eur} EUR) "
                        f"exceeds the property reference price, forcing an "
                        f"uncompetitive price. A minimum stay of "
                        f"{candidate.stay_length} nights dilutes the "
                        f"one-time booking cost enough to clear it "
                        f"({floor_relief_eur} EUR/night floor relief) — a "
                        f"whole reservation at that length should be "
                        f"priced at {suggested_price_per_reservation_eur} "
                        f"EUR total."
                    ),
                    impact=floor_relief_eur,
                ),
            )

    longest = ordered[-1]
    residual_gap_eur = round(
        longest.minimum_price_eur - property_reference_price_eur, 2
    )
    return MinimumStayRecommendation(
        recommended_min_stay=None,
        floor_relief_eur=None,
        cost_per_reservation_eur=None,
        suggested_price_per_reservation_eur=None,
        decision_component=DecisionComponent(
            code="minimum_stay_not_viable",
            label=(
                f"Even at {longest.stay_length} nights, the cost floor "
                f"still exceeds the property reference price by "
                f"{residual_gap_eur} EUR — the fixed/variable cost base, "
                f"not the one-time booking cost, is the binding "
                f"constraint. A minimum-stay policy alone will not fix "
                f"this."
            ),
            impact=residual_gap_eur,
        ),
    )


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
