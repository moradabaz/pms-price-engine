from flink_jobs.decision_components import DecisionComponent
from flink_jobs.pricing import (
    LOS_CANDIDATES,
    commission_base_netting_component,
    decide_price,
    decide_price_los_matrix,
    netted_commission_amount,
    rule_decision_component,
)


def test_division_not_multiplication_for_the_floor():
    # The exact "common error" the stakeholders' own reference material
    # flags: cost * (1 + margin) = 140.0. Division gives 166.67 (ADR-0009 D1).
    result = decide_price(
        fixed_cost_eur=0.0,
        variable_cost_eur=100.0,
        one_time_cost_eur=0.0,
        target_margin=0.40,
        commission_pct=0.0,
        avg_nightly_rate_eur=1000.0,  # high enough that the floor always wins
        competitiveness_discount=0.0,
        days_to_arrival=45,
    )
    assert result.minimum_price_eur == 166.67
    assert result.minimum_price_eur != 140.0


def test_market_competitive_structural_full_margin():
    result = decide_price(
        fixed_cost_eur=0.0,
        variable_cost_eur=13.67,
        one_time_cost_eur=0.0,
        target_margin=0.2,
        commission_pct=0.15,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
        days_to_arrival=45,
    )
    assert result.floor_type == "structural_full_margin"
    assert result.rule_applied == "market_competitive"
    assert result.suggested_price_eur == result.market_reference_price_eur


def test_structural_reduced_margin_tier_uses_reduced_margin():
    full_margin = decide_price(
        fixed_cost_eur=0.0,
        variable_cost_eur=140.0,
        one_time_cost_eur=0.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=150.0,
        competitiveness_discount=0.05,
        days_to_arrival=45,
    )
    reduced_margin = decide_price(
        fixed_cost_eur=0.0,
        variable_cost_eur=140.0,
        one_time_cost_eur=0.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=150.0,
        competitiveness_discount=0.05,
        days_to_arrival=20,
    )
    assert reduced_margin.floor_type == "structural_reduced_margin"
    # 0.75 factor -> a lower floor than the full-margin tier, same inputs.
    assert reduced_margin.minimum_price_eur < full_margin.minimum_price_eur


def test_contribution_tier_ignores_fixed_cost():
    with_fixed = decide_price(
        fixed_cost_eur=500.0,
        variable_cost_eur=30.0,
        one_time_cost_eur=20.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
        days_to_arrival=10,
    )
    without_fixed = decide_price(
        fixed_cost_eur=0.0,
        variable_cost_eur=30.0,
        one_time_cost_eur=20.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
        days_to_arrival=10,
    )
    assert with_fixed.floor_type == "contribution"
    assert with_fixed.minimum_price_eur == without_fixed.minimum_price_eur
    assert with_fixed.minimum_price_eur == 58.82  # (30 + 20) / (1 - 0.15)


def test_cost_protected():
    result = decide_price(
        fixed_cost_eur=0.0,
        variable_cost_eur=100.0,
        one_time_cost_eur=0.0,
        target_margin=0.05,
        commission_pct=0.0,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
        days_to_arrival=45,
    )
    assert result.rule_applied == "cost_protected"
    assert result.below_market_by < 0


def test_below_market_by_sign_flips_between_rules():
    common = dict(
        fixed_cost_eur=0.0,
        variable_cost_eur=140.0,
        one_time_cost_eur=0.0,
        target_margin=0.05,
        commission_pct=0.0,
        competitiveness_discount=0.05,
        days_to_arrival=45,
    )
    floor = decide_price(avg_nightly_rate_eur=150.0, **common)
    protected = decide_price(avg_nightly_rate_eur=90.0, **common)
    assert floor.below_market_by > 0
    assert protected.below_market_by < 0


def test_property_attribute_factor_changes_rule_applied():
    # Same cost floor (210.0, structural_full_margin), same raw market rate
    # (200.0) — only the Bonus/Malus factor differs (ADR-0011 backlog #6,
    # spec 08 §4 worked example). one_time_cost_eur=199.5 is chosen so
    # minimum_price_eur lands exactly on 210.0: 199.5 / (1 - 0.05) = 210.0.
    common = dict(
        fixed_cost_eur=0.0,
        variable_cost_eur=0.0,
        one_time_cost_eur=199.5,
        target_margin=0.05,
        commission_pct=0.0,
        avg_nightly_rate_eur=200.0,
        competitiveness_discount=0.05,
        days_to_arrival=45,
    )
    neutral = decide_price(property_attribute_factor=1.0, **common)
    boosted = decide_price(property_attribute_factor=1.47, **common)

    assert neutral.minimum_price_eur == 210.0
    # 210.0 exceeds Apartment A's own (unboosted) market reference (200.0) —
    # its costs price it out of its own market.
    assert neutral.rule_applied == "cost_protected"
    assert neutral.suggested_price_eur == 210.0
    # Apartment B's boosted reference (294.0) comfortably clears the same floor.
    assert boosted.property_reference_price_eur == 294.0
    assert boosted.rule_applied == "market_competitive"
    assert boosted.suggested_price_eur > neutral.suggested_price_eur


def test_property_attribute_factor_defaults_to_neutral():
    common = dict(
        fixed_cost_eur=0.0,
        variable_cost_eur=13.67,
        one_time_cost_eur=0.0,
        target_margin=0.2,
        commission_pct=0.15,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
        days_to_arrival=45,
    )
    assert decide_price(**common) == decide_price(
        property_attribute_factor=1.0, **common
    )


def test_floor_type_boundaries():
    common = dict(
        fixed_cost_eur=50.0,
        variable_cost_eur=30.0,
        one_time_cost_eur=20.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
    )
    assert (
        decide_price(days_to_arrival=31, **common).floor_type
        == "structural_full_margin"
    )
    assert (
        decide_price(days_to_arrival=30, **common).floor_type
        == "structural_reduced_margin"
    )
    assert (
        decide_price(days_to_arrival=15, **common).floor_type
        == "structural_reduced_margin"
    )
    assert decide_price(days_to_arrival=14, **common).floor_type == "contribution"
    assert decide_price(days_to_arrival=0, **common).floor_type == "contribution"


def test_stay_length_default_matches_pre_phase_9_behavior():
    # AC-01: stay_length=1 (the default) must reproduce every existing
    # worked example unchanged.
    common = dict(
        fixed_cost_eur=0.0,
        variable_cost_eur=13.67,
        one_time_cost_eur=0.0,
        target_margin=0.2,
        commission_pct=0.15,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
        days_to_arrival=45,
    )
    assert decide_price(**common) == decide_price(stay_length=1, **common)


def test_stay_length_amortizes_one_time_cost_only():
    # AC-02: a longer stay dilutes Cr (one_time_cost_eur) exactly by 1/n —
    # this is the actual bug ADR-0009's literal (n*Cf)+(n*Cv)+Cr notation had
    # for n>1 (docs/phase-9-los-floor-matrix-design-decisions.md §B): it never
    # divides Cr by n, so a pure-Cr scenario like this one would never dilute
    # at all under the old formula.
    common = dict(
        fixed_cost_eur=0.0,
        variable_cost_eur=0.0,
        one_time_cost_eur=110.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=1000.0,  # high enough the floor always wins
        competitiveness_discount=0.0,
        days_to_arrival=45,
    )
    los_1 = decide_price(stay_length=1, **common)
    los_10 = decide_price(stay_length=10, **common)
    assert los_1.minimum_price_eur == round(110.0 / 0.80, 2)
    assert los_10.minimum_price_eur == round(11.0 / 0.80, 2)
    assert los_10.minimum_price_eur == round(los_1.minimum_price_eur / 10, 2)


def test_stay_length_never_scales_fixed_or_variable_cost():
    # AC-02: fixed_cost_eur/variable_cost_eur are already per-night rates
    # (cost_aggregation.py) — minimum_price_eur must be identical across
    # every stay_length when one_time_cost_eur is zero.
    common = dict(
        fixed_cost_eur=20.0,
        variable_cost_eur=15.0,
        one_time_cost_eur=0.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=1000.0,
        competitiveness_discount=0.0,
        days_to_arrival=45,
    )
    results = {
        n: decide_price(stay_length=n, **common).minimum_price_eur
        for n in LOS_CANDIDATES
    }
    assert len(set(results.values())) == 1


def test_decide_price_los_matrix_matches_standalone_calls():
    # AC-03: every matrix entry must equal a standalone decide_price() call
    # at that same stay_length.
    common = dict(
        fixed_cost_eur=3.55,
        variable_cost_eur=0.0,
        one_time_cost_eur=110.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
        days_to_arrival=45,
        property_attribute_factor=1.1,
    )
    matrix = decide_price_los_matrix(**common)
    assert [c.stay_length for c in matrix] == list(LOS_CANDIDATES)
    for candidate in matrix:
        standalone = decide_price(stay_length=candidate.stay_length, **common)
        assert candidate.minimum_price_eur == standalone.minimum_price_eur
        assert candidate.floor_type == standalone.floor_type
        assert candidate.rule_applied == standalone.rule_applied
        assert candidate.suggested_price_eur == standalone.suggested_price_eur
        assert candidate.effective_margin == standalone.effective_margin


def test_decide_price_los_matrix_rule_applied_can_differ_across_candidates():
    # AC-04: spec 09 §4's worked example — a one-night stay can't absorb a
    # one-time cost profitably, but the same cost diluted across a longer
    # stay clears the market reference.
    matrix = decide_price_los_matrix(
        fixed_cost_eur=0.0,
        variable_cost_eur=0.0,
        one_time_cost_eur=110.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
        days_to_arrival=45,
        stay_lengths=(1, 3, 7, 14),
    )
    by_stay_length = {c.stay_length: c for c in matrix}
    assert by_stay_length[1].minimum_price_eur == 137.5
    assert by_stay_length[1].rule_applied == "cost_protected"
    assert by_stay_length[1].suggested_price_eur == 137.5
    for n in (3, 7, 14):
        assert by_stay_length[n].rule_applied == "market_competitive"
        assert by_stay_length[n].suggested_price_eur == 85.5


def test_rule_decision_component_market_competitive():
    # spec 10 §3: impact is the headroom, market reference minus floor.
    component = rule_decision_component(
        "market_competitive",
        minimum_price_eur=21.03,
        market_reference_price_eur=114.47,
        property_reference_price_eur=120.5,
    )
    assert component.code == "rule_market_competitive"
    assert component.impact == 93.44


def test_rule_decision_component_minimum_floor():
    # impact is how far the floor sits above market, while still <= property
    # reference (property_reference=200.0 > minimum_price=195.0).
    component = rule_decision_component(
        "minimum_floor",
        minimum_price_eur=195.0,
        market_reference_price_eur=190.0,
        property_reference_price_eur=200.0,
    )
    assert component.code == "rule_minimum_floor"
    assert component.impact == 5.0


def test_rule_decision_component_cost_protected():
    # impact is how far the floor exceeds the apartment's own reference.
    component = rule_decision_component(
        "cost_protected",
        minimum_price_eur=210.0,
        market_reference_price_eur=190.0,
        property_reference_price_eur=200.0,
    )
    assert component.code == "rule_cost_protected"
    assert component.impact == 10.0


def test_decide_price_default_decision_components_is_rule_only():
    # AC-04: no property_decision_components passed -> exactly one entry.
    result = decide_price(
        fixed_cost_eur=0.0,
        variable_cost_eur=13.67,
        one_time_cost_eur=0.0,
        target_margin=0.2,
        commission_pct=0.15,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
        days_to_arrival=45,
    )
    assert len(result.decision_components) == 1
    component = result.decision_components[0]
    assert component.code == "rule_market_competitive"
    assert component.impact == round(
        result.market_reference_price_eur - result.minimum_price_eur, 2
    )


def test_decide_price_prepends_property_components_before_rule_component():
    # AC-04: property components, when passed, come first, in order, followed
    # by exactly one rule component — spec 08/10's two-apartment example.
    property_components = [
        DecisionComponent(code="property_quality_tier", label="x", impact=0.30),
        DecisionComponent(code="property_rating", label="x", impact=0.08),
        DecisionComponent(code="property_view", label="x", impact=0.05),
        DecisionComponent(code="property_parking", label="x", impact=0.04),
    ]
    result = decide_price(
        fixed_cost_eur=0.0,
        variable_cost_eur=0.0,
        one_time_cost_eur=199.5,
        target_margin=0.05,
        commission_pct=0.0,
        avg_nightly_rate_eur=200.0,
        competitiveness_discount=0.05,
        days_to_arrival=45,
        property_attribute_factor=1.47,
        property_decision_components=property_components,
    )
    assert result.decision_components[:4] == property_components
    assert result.decision_components[4].code == "rule_market_competitive"
    assert len(result.decision_components) == 5


def test_decide_price_los_matrix_candidates_carry_only_their_own_rule_component():
    # AC-05: every LosFloorCandidate.decision_components has exactly 1 entry,
    # matching its own rule_applied — never a property_* component, even
    # though property_attribute_factor=1.1 is non-neutral here.
    matrix = decide_price_los_matrix(
        fixed_cost_eur=0.0,
        variable_cost_eur=0.0,
        one_time_cost_eur=110.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
        days_to_arrival=45,
        property_attribute_factor=1.1,
        stay_lengths=(1, 3, 7, 14),
    )
    for candidate in matrix:
        assert len(candidate.decision_components) == 1
        assert candidate.decision_components[0].code == f"rule_{candidate.rule_applied}"


def test_netted_commission_amount_per_base():
    assert netted_commission_amount("total_revenue", 45.0, 25.0) == 0.0
    assert netted_commission_amount("revenue_minus_ota", 45.0, 25.0) == 45.0
    assert (
        netted_commission_amount("revenue_minus_ota_minus_cleaning", 45.0, 25.0)
        == 70.0
    )


def test_commission_base_netting_component_absent_when_net_is_zero():
    # AC-05: no component at total_revenue, nor for a netting base whose
    # underlying amount happens to be zero this period (no noise for no
    # information gain, spec 11 §F).
    assert commission_base_netting_component("total_revenue", 0.15, 0.0) is None
    assert (
        commission_base_netting_component("revenue_minus_ota", 0.15, 0.0) is None
    )


def test_commission_base_netting_component_present_when_net_is_positive():
    component = commission_base_netting_component("revenue_minus_ota", 0.15, 45.0)
    assert component is not None
    assert component.code == "commission_base_netting"
    assert component.impact == -6.75


def test_decide_price_commission_base_netting_reduces_the_floor():
    # AC-03: worked example (spec 11 §4) — Cf=20 (incl. channel_manager=15),
    # Cv=100 (incl. ota_fee=30, cleaning=25), margin=0.05, commission=0.15.
    common = dict(
        fixed_cost_eur=20.0,
        variable_cost_eur=100.0,
        one_time_cost_eur=0.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=1000.0,  # floor always wins
        competitiveness_discount=0.0,
        days_to_arrival=45,
    )
    total_revenue = decide_price(commission_netting_eur=0.0, **common)
    revenue_minus_ota = decide_price(commission_netting_eur=6.75, **common)
    revenue_minus_ota_minus_cleaning = decide_price(
        commission_netting_eur=10.50, **common
    )

    assert total_revenue.minimum_price_eur == 150.0
    assert revenue_minus_ota.minimum_price_eur == 141.56
    assert revenue_minus_ota_minus_cleaning.minimum_price_eur == 136.88
    # Narrower base -> lower floor, all else equal (sign-correctness check).
    assert (
        total_revenue.minimum_price_eur
        > revenue_minus_ota.minimum_price_eur
        > revenue_minus_ota_minus_cleaning.minimum_price_eur
    )


def test_decide_price_commission_netting_applies_to_all_three_floor_tiers():
    # AC-04: the same netting term appears in structural_full_margin,
    # structural_reduced_margin, and contribution alike.
    common = dict(
        fixed_cost_eur=20.0,
        variable_cost_eur=100.0,
        one_time_cost_eur=0.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=1000.0,
        competitiveness_discount=0.0,
    )
    for days_to_arrival, floor_type in (
        (45, "structural_full_margin"),
        (20, "structural_reduced_margin"),
        (5, "contribution"),
    ):
        without_netting = decide_price(
            days_to_arrival=days_to_arrival, commission_netting_eur=0.0, **common
        )
        with_netting = decide_price(
            days_to_arrival=days_to_arrival, commission_netting_eur=6.75, **common
        )
        assert without_netting.floor_type == floor_type
        assert with_netting.minimum_price_eur < without_netting.minimum_price_eur


def test_decide_price_default_commission_netting_is_a_pure_regression():
    # AC-02: commission_netting_eur=0.0 (the default) reproduces every
    # pre-Phase-11 worked example unchanged.
    common = dict(
        fixed_cost_eur=0.0,
        variable_cost_eur=13.67,
        one_time_cost_eur=0.0,
        target_margin=0.2,
        commission_pct=0.15,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
        days_to_arrival=45,
    )
    assert decide_price(**common) == decide_price(commission_netting_eur=0.0, **common)


def test_decide_price_commission_component_appended_after_property_and_before_rule():
    property_components = [
        DecisionComponent(code="property_quality_tier", label="x", impact=0.0),
    ]
    commission_component = commission_base_netting_component(
        "revenue_minus_ota", 0.15, 45.0
    )
    result = decide_price(
        fixed_cost_eur=20.0,
        variable_cost_eur=100.0,
        one_time_cost_eur=0.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=1000.0,
        competitiveness_discount=0.0,
        days_to_arrival=45,
        commission_netting_eur=6.75,
        property_decision_components=property_components,
        commission_decision_components=[commission_component],
    )
    assert [c.code for c in result.decision_components] == [
        "property_quality_tier",
        "commission_base_netting",
        "rule_market_competitive",
    ]


def test_decide_price_los_matrix_never_carries_commission_component():
    # AC-06: commission_netting_eur applies to every candidate's price, but
    # commission_base_netting is never duplicated per candidate — only the
    # top-level calculation carries it (spec 11 §F, mirrors Phase 10's own
    # property-component suppression).
    without_netting = decide_price_los_matrix(
        fixed_cost_eur=20.0,
        variable_cost_eur=100.0,
        one_time_cost_eur=0.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=1000.0,
        competitiveness_discount=0.0,
        days_to_arrival=45,
        commission_netting_eur=0.0,
    )
    with_netting = decide_price_los_matrix(
        fixed_cost_eur=20.0,
        variable_cost_eur=100.0,
        one_time_cost_eur=0.0,
        target_margin=0.05,
        commission_pct=0.15,
        avg_nightly_rate_eur=1000.0,
        competitiveness_discount=0.0,
        days_to_arrival=45,
        commission_netting_eur=6.75,
    )
    for candidate in with_netting:
        assert len(candidate.decision_components) == 1
        assert candidate.decision_components[0].code == f"rule_{candidate.rule_applied}"
    for baseline, netted in zip(without_netting, with_netting, strict=True):
        assert netted.minimum_price_eur < baseline.minimum_price_eur
