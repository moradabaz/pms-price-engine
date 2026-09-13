from unittest.mock import patch

from pricing_formulas.decision_components import DecisionComponent
from pricing_formulas.engine import (
    CHANNEL_COMMISSION_PCT,
    LOS_CANDIDATES,
    decide_price,
    decide_price_by_channel,
    decide_price_los_matrix,
    floor_policy_for,
    recommend_minimum_stay,
)
from pricing_formulas.layers.commercial import revenue_base_netting_component


def test_division_not_multiplication_for_the_floor():
    # The exact "common error" the stakeholders' own reference material
    # flags: cost * (1 + margin) = 140.0. Division gives 166.67 (ADR-0009 D1,
    # unchanged by the ADR-0013 rewrite onto BER/MPR).
    result = decide_price(
        fixed_and_allocated_costs_per_night_eur=100.0,
        per_booking_cost_eur=0.0,
        p=0.0,
        target_margin=0.40,
        avg_nightly_rate_eur=1000.0,  # high enough that the floor always wins
        competitiveness_discount=0.0,
    )
    assert result.minimum_price_eur == 166.67
    assert result.minimum_price_eur != 140.0


def test_market_competitive():
    result = decide_price(
        fixed_and_allocated_costs_per_night_eur=13.67,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.2,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
    )
    assert result.rule_applied == "market_competitive"
    assert result.suggested_price_eur == result.market_reference_price_eur


def test_no_cost_is_ever_excluded_from_the_floor():
    # ADR-0013 §1/§2: ADR-0009's "contribution floor excludes fixed cost, it's
    # already sunk" behavior is retired — every non-percentage CostDefinition
    # contributes to fixed_and_allocated_costs_eur unconditionally, regardless
    # of target_margin or any other input. A higher fixed_and_allocated
    # figure always raises the floor, full stop.
    lower = decide_price(
        fixed_and_allocated_costs_per_night_eur=30.0,
        per_booking_cost_eur=20.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
    )
    higher = decide_price(
        fixed_and_allocated_costs_per_night_eur=530.0,
        per_booking_cost_eur=20.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
    )
    assert higher.minimum_price_eur > lower.minimum_price_eur


def test_minimum_profitable_price():
    result = decide_price(
        fixed_and_allocated_costs_per_night_eur=100.0,
        per_booking_cost_eur=0.0,
        p=0.0,
        target_margin=0.05,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
    )
    assert result.rule_applied == "minimum_profitable_price"
    assert result.below_market_by < 0


def test_below_market_by_sign_flips_between_rules():
    common = dict(
        fixed_and_allocated_costs_per_night_eur=140.0,
        per_booking_cost_eur=0.0,
        p=0.0,
        target_margin=0.05,
        competitiveness_discount=0.05,
    )
    floor = decide_price(avg_nightly_rate_eur=150.0, **common)
    protected = decide_price(avg_nightly_rate_eur=90.0, **common)
    assert floor.below_market_by > 0
    assert protected.below_market_by < 0


def test_property_attribute_factor_changes_rule_applied():
    # Same cost floor (210.0), same raw market rate (200.0) — only the
    # Bonus/Malus factor differs (ADR-0011 backlog #6, spec 08 §4 worked
    # example). per_booking_cost_eur=199.5 is chosen so minimum_price_eur
    # lands exactly on 210.0: 199.5 / (1 - 0.05) = 210.0.
    common = dict(
        fixed_and_allocated_costs_per_night_eur=0.0,
        per_booking_cost_eur=199.5,
        p=0.0,
        target_margin=0.05,
        avg_nightly_rate_eur=200.0,
        competitiveness_discount=0.05,
    )
    neutral = decide_price(property_attribute_factor=1.0, **common)
    boosted = decide_price(property_attribute_factor=1.47, **common)

    assert neutral.minimum_price_eur == 210.0
    # 210.0 exceeds Apartment A's own (unboosted) market reference (200.0) —
    # its costs price it out of its own market.
    assert neutral.rule_applied == "minimum_profitable_price"
    assert neutral.suggested_price_eur == 210.0
    # Apartment B's boosted reference (294.0) comfortably clears the same floor.
    assert boosted.property_reference_price_eur == 294.0
    assert boosted.rule_applied == "market_competitive"
    assert boosted.suggested_price_eur > neutral.suggested_price_eur


def test_property_attribute_factor_defaults_to_neutral():
    common = dict(
        fixed_and_allocated_costs_per_night_eur=13.67,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.2,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
    )
    assert decide_price(**common) == decide_price(
        property_attribute_factor=1.0, **common
    )


def test_floor_policy_for_target_margin():
    # ADR-0013 §5: "hard" iff target_margin == 0 (BER == MPR), "soft"
    # otherwise — no longer derived from an antelación-tiered floor_type
    # (retired).
    assert floor_policy_for(0.0) == "hard"
    assert floor_policy_for(0.05) == "soft"
    assert floor_policy_for(0.20) == "soft"


def test_decide_price_sets_floor_policy_from_target_margin():
    common = dict(
        fixed_and_allocated_costs_per_night_eur=50.0,
        per_booking_cost_eur=20.0,
        p=0.15,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
    )
    hard = decide_price(target_margin=0.0, **common)
    soft = decide_price(target_margin=0.05, **common)
    assert hard.floor_policy == "hard"
    assert hard.break_even_revenue_eur == hard.profitable_floor_eur
    assert soft.floor_policy == "soft"
    assert soft.break_even_revenue_eur < soft.profitable_floor_eur


def test_break_even_and_profitable_floor_relationship():
    # AC-02 (spec 20): BER <= MPR <= suggested_price whenever the floor
    # doesn't win outright; MPR is always what minimum_price_eur equals.
    result = decide_price(
        fixed_and_allocated_costs_per_night_eur=20.0,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=1000.0,
        competitiveness_discount=0.0,
    )
    assert result.break_even_revenue_eur < result.profitable_floor_eur
    assert result.minimum_price_eur == result.profitable_floor_eur


def test_stay_length_default_matches_stay_length_one():
    common = dict(
        fixed_and_allocated_costs_per_night_eur=13.67,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.2,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
    )
    assert decide_price(**common) == decide_price(stay_length=1, **common)


def test_stay_length_amortizes_per_booking_cost_only():
    # AC-02: a longer stay dilutes per_booking_cost_eur exactly by 1/n.
    common = dict(
        fixed_and_allocated_costs_per_night_eur=0.0,
        per_booking_cost_eur=110.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=1000.0,  # high enough the floor always wins
        competitiveness_discount=0.0,
    )
    los_1 = decide_price(stay_length=1, **common)
    los_10 = decide_price(stay_length=10, **common)
    assert los_1.minimum_price_eur == round(110.0 / 0.80, 2)
    assert los_10.minimum_price_eur == round(11.0 / 0.80, 2)
    assert los_10.minimum_price_eur == round(los_1.minimum_price_eur / 10, 2)


def test_stay_length_never_scales_fixed_and_allocated_cost():
    # AC-02: fixed_and_allocated_costs_per_night_eur is already a per-night
    # rate — minimum_price_eur must be identical across every stay_length
    # when per_booking_cost_eur is zero.
    common = dict(
        fixed_and_allocated_costs_per_night_eur=35.0,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=1000.0,
        competitiveness_discount=0.0,
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
        fixed_and_allocated_costs_per_night_eur=3.55,
        per_booking_cost_eur=110.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
        property_attribute_factor=1.1,
    )
    matrix = decide_price_los_matrix(**common)
    assert [c.stay_length for c in matrix] == list(LOS_CANDIDATES)
    for candidate in matrix:
        standalone = decide_price(stay_length=candidate.stay_length, **common)
        assert candidate.minimum_price_eur == standalone.minimum_price_eur
        assert candidate.floor_policy == standalone.floor_policy
        assert candidate.rule_applied == standalone.rule_applied
        assert candidate.suggested_price_eur == standalone.suggested_price_eur
        assert candidate.effective_margin == standalone.effective_margin


def test_decide_price_los_matrix_floor_policy_is_consistent_across_candidates():
    # ADR-0013 §5: floor_policy depends only on target_margin, which is
    # constant across an entire matrix — every candidate must agree.
    matrix = decide_price_los_matrix(
        fixed_and_allocated_costs_per_night_eur=0.0,
        per_booking_cost_eur=110.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
        stay_lengths=(1, 3, 7, 14),
    )
    assert all(c.floor_policy == "soft" for c in matrix)


def test_decide_price_los_matrix_rule_applied_can_differ_across_candidates():
    # AC-04: spec 09 §4's worked example — a one-night stay can't absorb a
    # one-time cost profitably, but the same cost diluted across a longer
    # stay clears the market reference.
    matrix = decide_price_los_matrix(
        fixed_and_allocated_costs_per_night_eur=0.0,
        per_booking_cost_eur=110.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
        stay_lengths=(1, 3, 7, 14),
    )
    by_stay_length = {c.stay_length: c for c in matrix}
    assert by_stay_length[1].minimum_price_eur == 137.5
    assert by_stay_length[1].rule_applied == "minimum_profitable_price"
    assert by_stay_length[1].suggested_price_eur == 137.5
    for n in (3, 7, 14):
        assert by_stay_length[n].rule_applied == "market_competitive"
        assert by_stay_length[n].suggested_price_eur == 85.5


def test_decide_price_default_decision_components_is_rule_only():
    # AC-04: no property_decision_components passed -> exactly one entry.
    result = decide_price(
        fixed_and_allocated_costs_per_night_eur=13.67,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.2,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
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
        fixed_and_allocated_costs_per_night_eur=0.0,
        per_booking_cost_eur=199.5,
        p=0.0,
        target_margin=0.05,
        avg_nightly_rate_eur=200.0,
        competitiveness_discount=0.05,
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
        fixed_and_allocated_costs_per_night_eur=0.0,
        per_booking_cost_eur=110.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
        property_attribute_factor=1.1,
        stay_lengths=(1, 3, 7, 14),
    )
    for candidate in matrix:
        assert len(candidate.decision_components) == 1
        assert candidate.decision_components[0].code == f"rule_{candidate.rule_applied}"


def test_decide_price_revenue_base_netting_reduces_the_floor():
    # AC-03: worked example (spec 11 §4, generalized by ADR-0013 §3) —
    # fixed_and_allocated=120 (incl. channel_manager=20, ota_fee/cleaning
    # portions of the old variable=100), margin=0.05, p=0.15.
    common = dict(
        fixed_and_allocated_costs_per_night_eur=120.0,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=1000.0,  # floor always wins
        competitiveness_discount=0.0,
    )
    total_revenue = decide_price(netting_eur=0.0, **common)
    revenue_minus_ota = decide_price(netting_eur=6.75, **common)
    revenue_minus_ota_minus_cleaning = decide_price(netting_eur=10.50, **common)

    assert total_revenue.minimum_price_eur == 150.0
    assert revenue_minus_ota.minimum_price_eur == 141.56
    assert revenue_minus_ota_minus_cleaning.minimum_price_eur == 136.88
    # Narrower base -> lower floor, all else equal (sign-correctness check).
    assert (
        total_revenue.minimum_price_eur
        > revenue_minus_ota.minimum_price_eur
        > revenue_minus_ota_minus_cleaning.minimum_price_eur
    )


def test_decide_price_default_netting_is_a_pure_regression():
    # AC-02: netting_eur=0.0 (the default) reproduces every no-netting
    # worked example unchanged.
    common = dict(
        fixed_and_allocated_costs_per_night_eur=13.67,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.2,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
    )
    assert decide_price(**common) == decide_price(netting_eur=0.0, **common)


def test_decide_price_revenue_base_component_appended_after_property_and_before_rule():
    property_components = [
        DecisionComponent(code="property_quality_tier", label="x", impact=0.0),
    ]
    netting_component = revenue_base_netting_component("revenue_minus_ota", 0.15, 45.0)
    result = decide_price(
        fixed_and_allocated_costs_per_night_eur=120.0,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=1000.0,
        competitiveness_discount=0.0,
        netting_eur=6.75,
        property_decision_components=property_components,
        commission_decision_components=[netting_component],
    )
    assert [c.code for c in result.decision_components] == [
        "property_quality_tier",
        "revenue_base_netting",
        "rule_market_competitive",
    ]


def test_decide_price_los_matrix_never_carries_revenue_base_netting_component():
    # AC-06: netting_eur applies to every candidate's price, but
    # revenue_base_netting is never duplicated per candidate — only the
    # top-level calculation carries it (spec 11 §F, mirrors Phase 10's own
    # property-component suppression).
    without_netting = decide_price_los_matrix(
        fixed_and_allocated_costs_per_night_eur=120.0,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=1000.0,
        competitiveness_discount=0.0,
        netting_eur=0.0,
    )
    with_netting = decide_price_los_matrix(
        fixed_and_allocated_costs_per_night_eur=120.0,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=1000.0,
        competitiveness_discount=0.0,
        netting_eur=6.75,
    )
    for candidate in with_netting:
        assert len(candidate.decision_components) == 1
        assert candidate.decision_components[0].code == f"rule_{candidate.rule_applied}"
    for baseline, netted in zip(without_netting, with_netting, strict=True):
        assert netted.minimum_price_eur < baseline.minimum_price_eur


def test_rule_applied_boundary_survives_rounding_at_the_market_comparison():
    # AC-04 (spec 13 §F): minimum_price_eur (100.004 unrounded) is genuinely
    # ABOVE market_reference_price_eur (99.997 unrounded) -> not
    # market_competitive. Both round to 100.0 at 2dp, so a refactor that
    # rounds once and reuses that rounded value for the comparison (instead
    # of comparing unrounded, as apply_guardrails() must) would wrongly see
    # 100.0 <= 100.0 and pick market_competitive instead of minimum_profitable_price.
    result = decide_price(
        fixed_and_allocated_costs_per_night_eur=100.004,
        per_booking_cost_eur=0.0,
        p=0.0,
        target_margin=0.0,
        avg_nightly_rate_eur=99.997,
        competitiveness_discount=0.0,
    )
    assert result.rule_applied == "minimum_profitable_price"
    assert result.minimum_price_eur == 100.0
    assert result.market_reference_price_eur == 100.0
    assert result.suggested_price_eur == 100.0


def test_rule_applied_boundary_survives_rounding_at_the_property_comparison():
    # AC-04 (spec 13 §F): minimum_price_eur (100.004 unrounded) is genuinely
    # ABOVE property_reference_price_eur (99.997 unrounded) -> minimum_profitable_price,
    # not minimum_floor, even though market_reference_price_eur (94.99715,
    # from the 5% discount) is comfortably below both, and minimum_price_eur/
    # property_reference_price_eur both round to the same 100.0 at 2dp. A
    # round-then-compare refactor would wrongly see 100.0 <= 100.0 and pick
    # minimum_floor.
    result = decide_price(
        fixed_and_allocated_costs_per_night_eur=100.004,
        per_booking_cost_eur=0.0,
        p=0.0,
        target_margin=0.0,
        avg_nightly_rate_eur=99.997,
        competitiveness_discount=0.05,
    )
    assert result.rule_applied == "minimum_profitable_price"
    assert result.minimum_price_eur == 100.0
    assert result.property_reference_price_eur == 100.0
    assert result.market_reference_price_eur == 95.0
    assert result.suggested_price_eur == 100.0


def test_performance_and_inventory_layers_multiply_into_property_reference_price():
    # AC-05: proves the stubs are real, reachable code — not dead. Patched
    # where decide_price() looks them up (pricing_formulas.engine), not
    # where they're defined (pricing_formulas.layers.performance/inventory) —
    # patching the definition would leave engine.py's already-bound
    # reference untouched.
    common = dict(
        fixed_and_allocated_costs_per_night_eur=100.0,
        per_booking_cost_eur=0.0,
        p=0.0,
        target_margin=0.05,
        avg_nightly_rate_eur=100.0,
        competitiveness_discount=0.0,
        property_attribute_factor=1.0,
    )
    baseline = decide_price(**common)
    with patch("pricing_formulas.engine.performance_layer", return_value=1.10):
        boosted = decide_price(**common)
    assert boosted.property_reference_price_eur == round(
        baseline.property_reference_price_eur * 1.10, 2
    )

    with patch("pricing_formulas.engine.inventory_layer", return_value=0.90):
        reduced = decide_price(**common)
    assert reduced.property_reference_price_eur == round(
        baseline.property_reference_price_eur * 0.90, 2
    )


def test_recommend_minimum_stay_already_fine_at_one_night():
    # AC-01: stay_length=1 not minimum_profitable_price -> recommend 1, no relief,
    # no decision component (nothing to explain).
    common = dict(
        fixed_and_allocated_costs_per_night_eur=13.67,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.2,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
    )
    matrix = decide_price_los_matrix(**common)
    calc = decide_price(**common)
    recommendation = recommend_minimum_stay(
        matrix,
        calc.property_reference_price_eur,
        fixed_and_allocated_costs_per_night_eur=13.67,
        per_booking_cost_eur=0.0,
    )
    assert recommendation.recommended_min_stay == 1
    assert recommendation.floor_relief_eur == 0.0
    # AC (reservation totals): at LOS 1, "the reservation" IS the single
    # night — cost/price per reservation match the standalone decide_price()
    # call exactly.
    assert recommendation.cost_per_reservation_eur == round(13.67, 2)
    assert (
        recommendation.suggested_price_per_reservation_eur == calc.suggested_price_eur
    )
    assert recommendation.decision_component is None


def test_recommend_minimum_stay_finds_shortest_non_minimum_profitable_price_candidate():
    # AC-02: spec 15 §4's worked example — LOS 1 is minimum_profitable_price, LOS 2 is
    # the shortest candidate that clears it.
    matrix = decide_price_los_matrix(
        fixed_and_allocated_costs_per_night_eur=0.0,
        per_booking_cost_eur=110.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
    )
    by_stay_length = {c.stay_length: c for c in matrix}
    assert by_stay_length[1].rule_applied == "minimum_profitable_price"
    assert by_stay_length[2].rule_applied == "market_competitive"

    recommendation = recommend_minimum_stay(
        matrix,
        property_reference_price_eur=90.0,
        fixed_and_allocated_costs_per_night_eur=0.0,
        per_booking_cost_eur=110.0,
    )

    assert recommendation.recommended_min_stay == 2
    assert recommendation.floor_relief_eur == round(137.5 - 68.75, 2)
    # AC (reservation totals): a 2-night reservation pays the one-time cost
    # once (110.0, no fixed/variable here) and should be priced at the LOS-2
    # candidate's own market-competitive per-night rate (85.5) x 2 nights.
    assert recommendation.cost_per_reservation_eur == 110.0
    assert recommendation.suggested_price_per_reservation_eur == round(85.5 * 2, 2)
    assert recommendation.decision_component is not None
    assert recommendation.decision_component.code == "minimum_stay_recommended"
    assert recommendation.decision_component.impact == recommendation.floor_relief_eur


def test_recommend_minimum_stay_not_viable_when_no_candidate_clears_the_floor():
    # AC-03: even the longest candidate (14 nights) stays minimum_profitable_price —
    # a structural fixed/variable cost problem, not a one-time-cost dilution
    # one. No recommendation exists.
    matrix = decide_price_los_matrix(
        fixed_and_allocated_costs_per_night_eur=90.0,
        per_booking_cost_eur=10.0,
        p=0.15,
        target_margin=0.05,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
    )
    assert all(c.rule_applied == "minimum_profitable_price" for c in matrix)

    recommendation = recommend_minimum_stay(
        matrix,
        property_reference_price_eur=90.0,
        fixed_and_allocated_costs_per_night_eur=90.0,
        per_booking_cost_eur=10.0,
    )

    assert recommendation.recommended_min_stay is None
    assert recommendation.floor_relief_eur is None
    assert recommendation.cost_per_reservation_eur is None
    assert recommendation.suggested_price_per_reservation_eur is None
    assert recommendation.decision_component is not None
    assert recommendation.decision_component.code == "minimum_stay_not_viable"
    longest = max(matrix, key=lambda c: c.stay_length)
    assert recommendation.decision_component.impact == round(
        longest.minimum_price_eur - 90.0, 2
    )


def test_minimum_price_eur_is_monotonically_non_increasing_in_stay_length():
    # AC-04: the correctness precondition recommend_minimum_stay() relies on
    # — checked directly, not just assumed, across several cost profiles.
    profiles = [
        (0.0, 110.0),
        (35.0, 0.0),
        (90.0, 10.0),
        (3.55, 110.0),
    ]
    for fixed_and_allocated_costs_per_night_eur, per_booking_cost_eur in profiles:
        matrix = decide_price_los_matrix(
            fixed_and_allocated_costs_per_night_eur=fixed_and_allocated_costs_per_night_eur,
            per_booking_cost_eur=per_booking_cost_eur,
            p=0.15,
            target_margin=0.05,
            avg_nightly_rate_eur=90.0,
            competitiveness_discount=0.05,
        )
        ordered = sorted(matrix, key=lambda c: c.stay_length)
        floors = [c.minimum_price_eur for c in ordered]
        assert floors == sorted(floors, reverse=True)

        rule_rank = {
            "minimum_profitable_price": 0,
            "minimum_floor": 1,
            "market_competitive": 2,
        }
        ranks = [rule_rank[c.rule_applied] for c in ordered]
        assert ranks == sorted(ranks)


def test_decide_price_by_channel_empty_input_returns_empty_list():
    # AC-01: no channel snapshot has reached this night yet.
    assert (
        decide_price_by_channel(
            channel_rates_eur={},
            fixed_and_allocated_costs_per_night_eur=13.67,
            per_booking_cost_eur=0.0,
            target_margin=0.2,
            competitiveness_discount=0.05,
        )
        == []
    )


def test_decide_price_by_channel_matches_standalone_decide_price():
    # AC-02: one channel's candidate is exactly what a standalone
    # decide_price() call with that channel's own rate/commission produces.
    candidates = decide_price_by_channel(
        channel_rates_eur={"airbnb": 97.2},
        fixed_and_allocated_costs_per_night_eur=13.67,
        per_booking_cost_eur=0.0,
        target_margin=0.2,
        competitiveness_discount=0.05,
    )
    assert len(candidates) == 1
    candidate = candidates[0]

    standalone = decide_price(
        fixed_and_allocated_costs_per_night_eur=13.67,
        per_booking_cost_eur=0.0,
        p=CHANNEL_COMMISSION_PCT["airbnb"],
        target_margin=0.2,
        avg_nightly_rate_eur=97.2,
        competitiveness_discount=0.05,
    )
    assert candidate.platform == "airbnb"
    assert candidate.avg_nightly_rate_eur == 97.2
    assert candidate.commission_pct == CHANNEL_COMMISSION_PCT["airbnb"]
    assert candidate.market_reference_price_eur == standalone.market_reference_price_eur
    assert candidate.minimum_price_eur == standalone.minimum_price_eur
    assert candidate.rule_applied == standalone.rule_applied
    assert candidate.suggested_price_eur == standalone.suggested_price_eur
    assert candidate.effective_margin == standalone.effective_margin


def test_decide_price_by_channel_rule_applied_can_differ_per_channel():
    # AC-03: unlike LOS's single monotonic axis, channel candidates can land
    # on different rule_applied values independently of each other — here
    # airbnb's own market rate clears the floor easily, booking's own rate
    # is too low relative to the same variable cost to ever clear it.
    candidates = decide_price_by_channel(
        channel_rates_eur={"airbnb": 200.0, "booking": 3.0},
        fixed_and_allocated_costs_per_night_eur=5.0,
        per_booking_cost_eur=0.0,
        target_margin=0.05,
        competitiveness_discount=0.05,
    )
    by_platform = {c.platform: c for c in candidates}
    assert by_platform["airbnb"].rule_applied == "market_competitive"
    assert by_platform["booking"].rule_applied == "minimum_profitable_price"


def test_decide_price_by_channel_top_level_calculation_is_unaffected():
    # AC-04: decide_price() itself takes no channel-related parameters —
    # adding channel candidates alongside it cannot change its output.
    before = decide_price(
        fixed_and_allocated_costs_per_night_eur=13.67,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.2,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
    )
    decide_price_by_channel(
        channel_rates_eur={"airbnb": 130.0, "booking": 110.0, "vrbo": 100.0},
        fixed_and_allocated_costs_per_night_eur=13.67,
        per_booking_cost_eur=0.0,
        target_margin=0.2,
        competitiveness_discount=0.05,
    )
    after = decide_price(
        fixed_and_allocated_costs_per_night_eur=13.67,
        per_booking_cost_eur=0.0,
        p=0.15,
        target_margin=0.2,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
    )
    assert before == after
