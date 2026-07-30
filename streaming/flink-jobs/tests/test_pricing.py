from flink_jobs.pricing import decide_price


def test_market_competitive():
    result = decide_price(
        daily_cost_eur=13.67,
        target_margin=0.2,
        avg_nightly_rate_eur=120.5,
        competitiveness_discount=0.05,
    )
    assert result.rule_applied == "market_competitive"
    assert result.suggested_price_eur == result.market_reference_price_eur


def test_minimum_floor():
    result = decide_price(
        daily_cost_eur=140.0,
        target_margin=0.05,
        avg_nightly_rate_eur=150.0,
        competitiveness_discount=0.05,
    )
    assert result.rule_applied == "minimum_floor"
    assert result.suggested_price_eur == 147.0
    assert result.below_market_by == 3.0


def test_cost_protected():
    result = decide_price(
        daily_cost_eur=100.0,
        target_margin=0.05,
        avg_nightly_rate_eur=90.0,
        competitiveness_discount=0.05,
    )
    assert result.rule_applied == "cost_protected"
    assert result.suggested_price_eur == 105.0
    assert result.below_market_by == -15.0


def test_below_market_by_sign_flips_between_rules():
    floor = decide_price(140.0, 0.05, 150.0, 0.05)
    protected = decide_price(140.0, 0.05, 90.0, 0.05)
    assert floor.below_market_by > 0
    assert protected.below_market_by < 0
