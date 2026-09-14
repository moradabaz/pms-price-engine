from pricing_formulas.viability import (
    PERSISTENT_BREACH_THRESHOLD_DAYS,
    classify_viability,
)


def test_ok_when_market_competitive_below_property_reference():
    result = classify_viability(
        rule_applied="market_competitive",
        below_market_by=6.03,
        recommended_min_stay=1,
        any_channel_market_competitive=False,
        manual_override_active=False,
        floor_breach_days=0,
    )
    assert result == "ok"


def test_demand_upside_when_suggested_at_or_above_property_reference():
    result = classify_viability(
        rule_applied="market_competitive",
        below_market_by=0.0,
        recommended_min_stay=1,
        any_channel_market_competitive=False,
        manual_override_active=False,
        floor_breach_days=0,
    )
    assert result == "demand_upside"

    negative = classify_viability(
        rule_applied="market_competitive",
        below_market_by=-5.0,
        recommended_min_stay=1,
        any_channel_market_competitive=False,
        manual_override_active=False,
        floor_breach_days=0,
    )
    assert negative == "demand_upside"


def test_min_stay_lever_available_when_a_longer_stay_clears_the_floor():
    result = classify_viability(
        rule_applied="minimum_profitable_price",
        below_market_by=-30.0,
        recommended_min_stay=3,
        any_channel_market_competitive=False,
        manual_override_active=False,
        floor_breach_days=0,
    )
    assert result == "min_stay_lever_available"


def test_recommended_min_stay_of_one_is_not_a_lever():
    # recommend_minimum_stay() also returns recommended_min_stay=1 (not
    # None) when rule_applied == "minimum_floor" (already "fine" at 1
    # night, just not market_competitive) — spec 23 §7 correction.
    result = classify_viability(
        rule_applied="minimum_floor",
        below_market_by=-2.0,
        recommended_min_stay=1,
        any_channel_market_competitive=False,
        manual_override_active=False,
        floor_breach_days=0,
    )
    assert result == "floor_binding"


def test_channel_lever_available_when_no_min_stay_lever_but_a_channel_clears_it():
    result = classify_viability(
        rule_applied="minimum_profitable_price",
        below_market_by=-30.0,
        recommended_min_stay=None,
        any_channel_market_competitive=True,
        manual_override_active=False,
        floor_breach_days=0,
    )
    assert result == "channel_lever_available"


def test_floor_binding_when_no_lever_and_no_alert_condition():
    result = classify_viability(
        rule_applied="minimum_profitable_price",
        below_market_by=-30.0,
        recommended_min_stay=None,
        any_channel_market_competitive=False,
        manual_override_active=False,
        floor_breach_days=5,
    )
    assert result == "floor_binding"


def test_persistent_floor_breach_at_threshold():
    below = classify_viability(
        rule_applied="minimum_profitable_price",
        below_market_by=-30.0,
        recommended_min_stay=None,
        any_channel_market_competitive=False,
        manual_override_active=False,
        floor_breach_days=PERSISTENT_BREACH_THRESHOLD_DAYS - 1,
    )
    assert below == "floor_binding"

    at_threshold = classify_viability(
        rule_applied="minimum_profitable_price",
        below_market_by=-30.0,
        recommended_min_stay=None,
        any_channel_market_competitive=False,
        manual_override_active=False,
        floor_breach_days=PERSISTENT_BREACH_THRESHOLD_DAYS,
    )
    assert at_threshold == "persistent_floor_breach"


def test_override_active_wins_even_over_persistent_floor_breach():
    # AC-01: priority ordering — override always wins.
    result = classify_viability(
        rule_applied="minimum_profitable_price",
        below_market_by=-30.0,
        recommended_min_stay=None,
        any_channel_market_competitive=False,
        manual_override_active=True,
        floor_breach_days=90,
    )
    assert result == "override_active"


def test_persistent_floor_breach_wins_over_a_lever():
    result = classify_viability(
        rule_applied="minimum_profitable_price",
        below_market_by=-30.0,
        recommended_min_stay=3,
        any_channel_market_competitive=True,
        manual_override_active=False,
        floor_breach_days=PERSISTENT_BREACH_THRESHOLD_DAYS,
    )
    assert result == "persistent_floor_breach"
