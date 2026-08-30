from flink_jobs.pricing import LOS_CANDIDATES, decide_price, decide_price_los_matrix


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
