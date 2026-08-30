from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from flink_jobs.decision_components import DecisionComponent

RuleApplied = Literal["market_competitive", "minimum_floor", "cost_protected"]
FloorType = Literal[
    "structural_full_margin", "structural_reduced_margin", "contribution"
]
# Phase 11 (ADR-0011 backlog #5, spec 11 §4).
CommissionBase = Literal[
    "total_revenue", "revenue_minus_ota", "revenue_minus_ota_minus_cleaning"
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
    decision_components: list[DecisionComponent]


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
    """Computes the suggested nightly price and which rule/floor applied
    (ADR-0009, ADR-0011 backlog #6/#1). Returns a PriceCalculation."""
    # Phase 9: fixed_cost_eur/variable_cost_eur are already per-night rates
    # (cost_aggregation.py divides by available_days) — they never scale with
    # stay_length. one_time_cost_eur (Cr) is a lump sum per booking and is the
    # only term amortized across the stay. For stay_length=1 this is
    # algebraically identical to the pre-Phase-9 formula.
    one_time_cost_per_night_eur = one_time_cost_eur / stay_length

    # Phase 11 (ADR-0011 backlog #5): charging commission_pct against a
    # netted base ("Revenue - OTA", etc.) instead of the full suggested
    # price nets commission_netting_eur (= commission_pct * netted amount)
    # out of every floor tier's numerator (spec 11 §4 derivation). 0.0 for
    # commission_base="total_revenue" reproduces every pre-Phase-11 formula
    # exactly.
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

    # Phase 10/11 (ADR-0011 backlog #4/#5): property_decision_components and
    # commission_decision_components are both empty for LOS-matrix candidates
    # (decide_price_los_matrix() never forwards either), so their
    # decision_components naturally ends up as just [rule_component] — no
    # second code path (spec 10 §E, spec 11 §F/AC-06). commission_netting_eur
    # itself (the numeric floor adjustment) IS still forwarded to every
    # candidate — only the component that explains it is top-level-only.
    rule_component = rule_decision_component(
        rule_applied,
        round(minimum_price_eur, 2),
        round(market_reference_price_eur, 2),
        round(property_reference_price_eur, 2),
    )
    decision_components = [
        *property_decision_components,
        *commission_decision_components,
        rule_component,
    ]

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
        decision_components=decision_components,
    )


@dataclass(frozen=True)
class LosFloorCandidate:
    stay_length: int
    minimum_price_eur: float
    floor_type: FloorType
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
                rule_applied=calc.rule_applied,
                suggested_price_eur=calc.suggested_price_eur,
                effective_margin=calc.effective_margin,
                decision_components=calc.decision_components,
            )
        )
    return candidates
