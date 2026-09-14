from pricing_formulas.layers.booking_window import (
    booking_window_component,
    booking_window_factor,
)

# Tier table (spec 22 §3, BOOKING_WINDOW_TIERS):
#   >= 45 days: standard_window, factor 1.0
#   15-44 days: early_bird,     factor 0.97
#   3-14  days: standard_window, factor 1.0
#   1-2   days: last_minute,    factor 0.95
#   0     days: same_day,       factor 0.92


def test_factor_matches_tier_table():
    assert booking_window_factor(0) == 0.92  # same_day
    assert booking_window_factor(2) == 0.95  # last_minute
    assert booking_window_factor(10) == 1.0  # standard_window
    assert booking_window_factor(30) == 0.97  # early_bird
    assert booking_window_factor(60) == 1.0  # standard_window


def test_factor_boundary_44_vs_45_days():
    assert booking_window_factor(44) == 0.97  # early_bird
    assert booking_window_factor(45) == 1.0  # standard_window


def test_factor_boundary_14_vs_15_days():
    assert booking_window_factor(14) == 1.0  # standard_window
    assert booking_window_factor(15) == 0.97  # early_bird


def test_factor_boundary_2_vs_3_days():
    assert booking_window_factor(2) == 0.95  # last_minute
    assert booking_window_factor(3) == 1.0  # standard_window


def test_factor_boundary_0_vs_1_day():
    assert booking_window_factor(0) == 0.92  # same_day
    assert booking_window_factor(1) == 0.95  # last_minute


def test_factor_clamps_negative_days_to_arrival():
    assert booking_window_factor(-5) == booking_window_factor(0)


def test_component_none_for_standard_window():
    # spec 22 §5 AC-03: no component for either standard_window tier.
    assert booking_window_component(45) is None
    assert booking_window_component(10) is None


def test_component_present_for_non_zero_tiers():
    early_bird = booking_window_component(20)
    assert early_bird is not None
    assert early_bird.code == "rule_early_bird"
    assert early_bird.impact == -0.03

    last_minute = booking_window_component(1)
    assert last_minute is not None
    assert last_minute.code == "rule_last_minute"
    assert last_minute.impact == -0.05

    same_day = booking_window_component(0)
    assert same_day is not None
    assert same_day.code == "rule_same_day"
    assert same_day.impact == -0.08
