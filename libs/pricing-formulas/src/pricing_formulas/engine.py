from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from pricing_formulas.decision_components import DecisionComponent
from pricing_formulas.layers.commercial import RevenueBase, netted_revenue_base_amount
from pricing_formulas.layers.guardrails import RuleApplied, apply_guardrails
from pricing_formulas.layers.inventory import inventory_layer
from pricing_formulas.layers.market import market_reference_price
from pricing_formulas.layers.performance import performance_layer
from pricing_formulas.layers.structural import property_reference_price

# Phase 9 (ADR-0011 backlog #1, docs/phase-9-los-floor-matrix-design-decisions.md
# §C): candidate stay lengths for the LOS floor matrix.
LOS_CANDIDATES: tuple[int, ...] = (1, 2, 3, 7, 14)

# Phase 12 (ADR-0011 backlog #10): the external spec's Hard/Soft floor
# vocabulary. Phase 20 (ADR-0013 §5): retired ADR-0009's antelación-tiered
# floor_type as the source of this classification — there is only one flat
# floor formula now (Break-Even / Profitable Floor, external spec §11),
# evaluated identically regardless of days_to_arrival. "hard" iff
# target_margin == 0 (break_even_revenue_eur == profitable_floor_eur, the
# absolute never-crossed floor); "soft" whenever a margin is being targeted
# (relaxable, at most down to break-even).
FloorPolicy = Literal["hard", "soft"]


def floor_policy_for(target_margin: float) -> FloorPolicy:
    """Pure classification (ADR-0013 §5). Returns the policy."""
    return "hard" if target_margin == 0 else "soft"


@dataclass(frozen=True)
class PriceCalculation:
    minimum_price_eur: float
    break_even_revenue_eur: float
    profitable_floor_eur: float
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
    fixed_and_allocated_costs_per_night_eur: float,
    per_booking_cost_eur: float,
    p: float,
    target_margin: float,
    avg_nightly_rate_eur: float,
    competitiveness_discount: float,
    property_attribute_factor: float = 1.0,
    stay_length: int = 1,
    netting_eur: float = 0.0,
    property_decision_components: Sequence[DecisionComponent] = (),
    commission_decision_components: Sequence[DecisionComponent] = (),
) -> PriceCalculation:
    """Orchestrates the layered Revenue Management engine (ADR-0011 backlog
    #7, spec 13; rewired onto the external spec's own Break-Even/Profitable
    Floor formula by ADR-0013, spec 20 §3): Structural -> Performance ->
    Inventory -> Market -> Break-Even/Profitable Floor -> Guardrails.
    `p` is the summed rate of every applicable percentage CostDefinition for
    this apartment/period (owner commission included, ADR-0013 §4) —
    resolved once per decision by the caller (Stage B), not here.
    `netting_eur` is the already-netted EUR amount to subtract from the
    numerator (generalizing Phase 11's commission-base netting to any
    percentage CostDefinition with a revenue_base, ADR-0013 §3), likewise
    pre-computed by the caller. Structural's own attribute factor is
    likewise pre-resolved by the caller — computed once per apartment per
    segment-broadcast update in Stage A, not per decision; only its
    per-decision half (property_reference_price) runs inside this pipeline.
    Returns a PriceCalculation."""
    # Phase 9: fixed_and_allocated_costs_per_night_eur is already a per-night
    # rate (Flink's cost aggregation divides by available_days) — it never
    # scales with stay_length. per_booking_cost_eur (allocation_method=
    # 'booking') is a lump sum per booking and is the only term amortized
    # across the stay. For stay_length=1 this is algebraically identical to
    # the per-booking-unamortized figure.
    per_booking_cost_per_night_eur = per_booking_cost_eur / stay_length
    fixed_and_allocated_costs_eur = (
        fixed_and_allocated_costs_per_night_eur + per_booking_cost_per_night_eur
    )

    # External spec §11: BER = Fixed-and-allocated Costs / (1 - p); MPR =
    # Fixed-and-allocated Costs / (1 - p - m). netting_eur (ADR-0011 backlog
    # #5, generalized by ADR-0013 §3) reduces the numerator in both — a
    # percentage cost charged against a smaller base than the full price
    # needs less of the raw cost recovered from that percentage's own share.
    numerator_eur = fixed_and_allocated_costs_eur - netting_eur
    break_even_revenue_eur = numerator_eur / (1 - p) if p != 1 else 0.0
    mpr_denominator = 1 - p - target_margin
    profitable_floor_eur = (
        numerator_eur / mpr_denominator if mpr_denominator else 0.0
    )
    # ADR-0013 §5: MPR is always the enforced floor — BER is informational/
    # explainability only (external spec §14), never substituted in here.
    minimum_price_eur = profitable_floor_eur

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
        minimum_price_eur,
        market_reference_price_eur,
        property_reference_price_eur,
    )

    effective_margin = (
        (guardrails.suggested_price_eur / fixed_and_allocated_costs_eur) - 1
        if fixed_and_allocated_costs_eur
        else 0.0
    )

    # Phase 10/11 (ADR-0011 backlog #4/#5): property_decision_components and
    # commission_decision_components are both empty for LOS-matrix candidates
    # (decide_price_los_matrix() never forwards either), so their
    # decision_components naturally ends up as just [rule_component] — no
    # second code path (spec 10 §E, spec 11 §F/AC-06). netting_eur itself
    # (the numeric floor adjustment) IS still forwarded to every candidate —
    # only the component that explains it is top-level-only.
    decision_components = [
        *property_decision_components,
        *commission_decision_components,
        guardrails.rule_component,
    ]

    return PriceCalculation(
        minimum_price_eur=round(minimum_price_eur, 2),
        break_even_revenue_eur=round(break_even_revenue_eur, 2),
        profitable_floor_eur=round(profitable_floor_eur, 2),
        floor_policy=floor_policy_for(target_margin),
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
    fixed_and_allocated_costs_per_night_eur: float,
    per_booking_cost_eur: float,
) -> float:
    """Total cost for a whole reservation of stay_length nights — the
    inverse of decide_price()'s per-night amortization (spec 15
    §4-follow-up, ADR-0013): fixed_and_allocated_costs_per_night_eur recurs
    every night, per_booking_cost_eur is paid once per booking, not per
    night. Returns the rounded total."""
    return round(
        stay_length * fixed_and_allocated_costs_per_night_eur + per_booking_cost_eur, 2
    )


def recommend_minimum_stay(
    candidates: list[LosFloorCandidate],
    property_reference_price_eur: float,
    fixed_and_allocated_costs_per_night_eur: float,
    per_booking_cost_eur: float,
) -> MinimumStayRecommendation:
    """Minimum Stay as a profitability lever (ADR-0011 backlog #12, spec 15
    §4). Pure post-processing over an already-computed los_floor_matrix, plus
    the same raw cost inputs decide_price_los_matrix() itself received — no
    new formula duplicated. Relies on minimum_price_eur being monotonically
    non-increasing in stay_length (only per_booking_cost_eur / n varies with
    n; market_reference_price_eur/property_reference_price_eur are constant
    across candidates in one decision), so rule_applied can only move
    minimum_profitable_price -> minimum_floor -> market_competitive as n
    grows, never backwards — the shortest non-minimum_profitable_price
    candidate is therefore the unique correct threshold, not a heuristic.
    Alongside that stay length, also surfaces what the whole reservation
    would cost and what it should be priced at in total — a price already
    guaranteed to clear the cost floor and be at/below the market reference,
    since it's exactly the LOS candidate's own suggested_price_eur (never
    itself minimum_profitable_price) multiplied by the nights it covers.
    Returns the recommendation."""
    ordered = sorted(candidates, key=lambda c: c.stay_length)
    at_one_night = ordered[0]

    if at_one_night.rule_applied != "minimum_profitable_price":
        return MinimumStayRecommendation(
            recommended_min_stay=at_one_night.stay_length,
            floor_relief_eur=0.0,
            cost_per_reservation_eur=_reservation_cost_eur(
                at_one_night.stay_length,
                fixed_and_allocated_costs_per_night_eur,
                per_booking_cost_eur,
            ),
            suggested_price_per_reservation_eur=round(
                at_one_night.suggested_price_eur * at_one_night.stay_length, 2
            ),
            decision_component=None,
        )

    for candidate in ordered[1:]:
        if candidate.rule_applied != "minimum_profitable_price":
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
                    fixed_and_allocated_costs_per_night_eur,
                    per_booking_cost_eur,
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
    fixed_and_allocated_costs_per_night_eur: float,
    per_booking_cost_eur: float,
    p: float,
    target_margin: float,
    avg_nightly_rate_eur: float,
    competitiveness_discount: float,
    property_attribute_factor: float = 1.0,
    netting_eur: float = 0.0,
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
            fixed_and_allocated_costs_per_night_eur=fixed_and_allocated_costs_per_night_eur,
            per_booking_cost_eur=per_booking_cost_eur,
            p=p,
            target_margin=target_margin,
            avg_nightly_rate_eur=avg_nightly_rate_eur,
            competitiveness_discount=competitiveness_discount,
            property_attribute_factor=property_attribute_factor,
            stay_length=stay_length,
            netting_eur=netting_eur,
        )
        candidates.append(
            LosFloorCandidate(
                stay_length=stay_length,
                minimum_price_eur=calc.minimum_price_eur,
                floor_policy=calc.floor_policy,
                rule_applied=calc.rule_applied,
                suggested_price_eur=calc.suggested_price_eur,
                effective_margin=calc.effective_margin,
                decision_components=calc.decision_components,
            )
        )
    return candidates


# Phase 16 (ADR-0011 backlog #2): fixed per-platform commission, one value
# for every apartment alike — a deliberate cheap first step (spec 16 §2),
# not a real owner/channel contract. Plausible OTA commission figures, not
# sourced from a specific contract (same caveat LOS_CANDIDATES/segment
# multipliers already carry).
CHANNEL_COMMISSION_PCT: dict[str, float] = {
    "airbnb": 0.12,
    "booking": 0.15,
    "vrbo": 0.08,
}


@dataclass(frozen=True)
class ChannelPriceCandidate:
    platform: str
    avg_nightly_rate_eur: float
    commission_pct: float
    market_reference_price_eur: float
    minimum_price_eur: float
    floor_policy: FloorPolicy
    rule_applied: RuleApplied
    suggested_price_eur: float
    effective_margin: float
    decision_components: list[DecisionComponent]


def decide_price_by_channel(
    channel_rates_eur: dict[str, float],
    fixed_and_allocated_costs_per_night_eur: float,
    per_booking_cost_eur: float,
    target_margin: float,
    competitiveness_discount: float,
    property_attribute_factor: float = 1.0,
    p_other: float = 0.0,
    revenue_base: RevenueBase = "total_revenue",
    ota_related_cost_eur: float = 0.0,
    cleaning_cost_eur: float = 0.0,
    channel_commission_pct: dict[str, float] = CHANNEL_COMMISSION_PCT,
) -> list[ChannelPriceCandidate]:
    """Evaluates decide_price() once per known channel (ADR-0011 backlog #2),
    at stay_length=1. Unlike decide_price_los_matrix(), this is NOT a pure
    post-processing pass over already-computed numbers: each channel has its
    own real avg_nightly_rate_eur, which flows through property_reference_
    price()/market_reference_price() before it reaches the floor comparison
    (spec 16 §4) — so market_reference_price_eur genuinely varies per
    candidate here, unlike its identical-across-candidates LOS counterpart.
    `p_other` is the rate of every applicable percentage CostDefinition
    EXCLUDING this channel's own commission (e.g. ota_fee) — combined with
    each channel's own fixed commission constant to form that candidate's
    `p` (ADR-0013 leaves the channel gross-up mechanism itself unchanged,
    spec 20 §2). Still no formula duplicated — a thin per-channel
    composition over decide_price(), same discipline decide_price_los_matrix()
    follows. Produces exactly one candidate per key present in
    channel_rates_eur — never invents a channel with no observed market
    rate. Returns one ChannelPriceCandidate per known channel."""
    net = netted_revenue_base_amount(
        revenue_base, ota_related_cost_eur, cleaning_cost_eur
    )
    candidates = []
    for platform, avg_nightly_rate_eur in channel_rates_eur.items():
        commission_pct = channel_commission_pct[platform]
        netting_eur = round(commission_pct * net, 2)
        calc = decide_price(
            fixed_and_allocated_costs_per_night_eur=fixed_and_allocated_costs_per_night_eur,
            per_booking_cost_eur=per_booking_cost_eur,
            p=p_other + commission_pct,
            target_margin=target_margin,
            avg_nightly_rate_eur=avg_nightly_rate_eur,
            competitiveness_discount=competitiveness_discount,
            property_attribute_factor=property_attribute_factor,
            netting_eur=netting_eur,
        )
        candidates.append(
            ChannelPriceCandidate(
                platform=platform,
                avg_nightly_rate_eur=avg_nightly_rate_eur,
                commission_pct=commission_pct,
                market_reference_price_eur=calc.market_reference_price_eur,
                minimum_price_eur=calc.minimum_price_eur,
                floor_policy=calc.floor_policy,
                rule_applied=calc.rule_applied,
                suggested_price_eur=calc.suggested_price_eur,
                effective_margin=calc.effective_margin,
                decision_components=calc.decision_components,
            )
        )
    return candidates
