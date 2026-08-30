from flink_jobs.property_attributes import (
    MAX_FACTOR,
    MIN_FACTOR,
    property_attribute_factor,
)


def test_standard_neutral_attributes_yield_factor_one():
    factor = property_attribute_factor(
        quality_tier="standard", rating=4.0, has_view=False, has_parking=False
    )
    assert factor == 1.0


def test_each_quality_tier_shifts_the_factor():
    factors = {
        tier: property_attribute_factor(
            quality_tier=tier, rating=4.0, has_view=False, has_parking=False
        )
        for tier in ("basic", "standard", "premium", "luxury")
    }
    assert (
        factors["basic"] < factors["standard"] < factors["premium"] < factors["luxury"]
    )


def test_rating_above_and_below_baseline():
    above = property_attribute_factor(
        quality_tier="standard", rating=5.0, has_view=False, has_parking=False
    )
    below = property_attribute_factor(
        quality_tier="standard", rating=3.0, has_view=False, has_parking=False
    )
    assert above > 1.0
    assert below < 1.0


def test_view_and_parking_each_add_a_bonus():
    base = property_attribute_factor(
        quality_tier="standard", rating=4.0, has_view=False, has_parking=False
    )
    with_view = property_attribute_factor(
        quality_tier="standard", rating=4.0, has_view=True, has_parking=False
    )
    with_both = property_attribute_factor(
        quality_tier="standard", rating=4.0, has_view=True, has_parking=True
    )
    assert with_view > base
    assert with_both > with_view


def test_factor_stays_within_bounds_at_the_extremes():
    # Neither extreme actually reaches MIN_FACTOR/MAX_FACTOR with today's
    # weights and rating's [1.0, 5.0] DB constraint — the clamp is a
    # precautionary guardrail (pre-spec §B), not something these inputs
    # trigger. This test documents that bound, not the clamp branch itself.
    assert (
        property_attribute_factor(
            quality_tier="basic", rating=1.0, has_view=False, has_parking=False
        )
        >= MIN_FACTOR
    )
    assert (
        property_attribute_factor(
            quality_tier="luxury", rating=5.0, has_view=True, has_parking=True
        )
        <= MAX_FACTOR
    )
