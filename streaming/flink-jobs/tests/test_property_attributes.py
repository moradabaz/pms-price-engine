from flink_jobs.property_attributes import (
    MAX_FACTOR,
    MIN_FACTOR,
    PARKING_ADJUSTMENT,
    QUALITY_TIER_ADJUSTMENTS,
    RATING_BASELINE,
    RATING_WEIGHT,
    VIEW_ADJUSTMENT,
    property_attribute_components,
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


def test_components_always_returns_four_entries_even_at_zero_impact():
    # spec 10 §3: all four property_* components are always emitted, even
    # when an attribute contributes nothing (standard tier, baseline rating,
    # no view, no parking).
    components = property_attribute_components(
        quality_tier="standard", rating=4.0, has_view=False, has_parking=False
    )
    assert [c.code for c in components] == [
        "property_quality_tier",
        "property_rating",
        "property_view",
        "property_parking",
    ]
    assert [c.impact for c in components] == [0.0, 0.0, 0.0, 0.0]


def test_components_quality_tier_impact_matches_the_named_constant():
    for tier, adjustment in QUALITY_TIER_ADJUSTMENTS.items():
        components = property_attribute_components(
            quality_tier=tier, rating=4.0, has_view=False, has_parking=False
        )
        assert components[0].code == "property_quality_tier"
        assert components[0].impact == adjustment


def test_components_rating_impact_is_linear_around_the_baseline():
    components = property_attribute_components(
        quality_tier="standard", rating=4.8, has_view=False, has_parking=False
    )
    assert components[1].code == "property_rating"
    assert components[1].impact == round((4.8 - RATING_BASELINE) * RATING_WEIGHT, 4)


def test_components_view_and_parking_impact_only_when_true():
    components = property_attribute_components(
        quality_tier="standard", rating=4.0, has_view=True, has_parking=True
    )
    assert components[2].code == "property_view"
    assert components[2].impact == VIEW_ADJUSTMENT
    assert components[3].code == "property_parking"
    assert components[3].impact == PARKING_ADJUSTMENT

    no_amenities = property_attribute_components(
        quality_tier="standard", rating=4.0, has_view=False, has_parking=False
    )
    assert no_amenities[2].impact == 0.0
    assert no_amenities[3].impact == 0.0


def test_factor_is_derived_from_components_sum_not_a_separate_formula():
    # AC-02: property_attribute_factor()'s refactor must be a pure regression
    # — its result is exactly 1 + sum(component impacts), clamped. Verified
    # against the luxury worked example (spec 08/10 §4): 0.30+0.08+0.05+0.04.
    components = property_attribute_components(
        quality_tier="luxury", rating=4.8, has_view=True, has_parking=True
    )
    expected = round(
        min(max(1 + sum(c.impact for c in components), MIN_FACTOR), MAX_FACTOR), 4
    )
    actual = property_attribute_factor(
        quality_tier="luxury", rating=4.8, has_view=True, has_parking=True
    )
    assert actual == expected == 1.47
