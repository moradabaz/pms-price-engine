from flink_jobs.pricing import decide_price


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
