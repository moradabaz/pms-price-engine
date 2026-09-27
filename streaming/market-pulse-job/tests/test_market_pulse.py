from datetime import date, datetime
from uuid import uuid4

from market_pulse_job.market_pulse import (
    MarketPulseAggregateFunction,
    is_blended_snapshot,
    segment_key,
)
from shared_schemas.market_price import (
    MarketArea,
    MarketContext,
    MarketPrice,
    Pricing,
    PropertyProfile,
)


def _market_price(
    *,
    city: str = "Barcelona",
    neighborhood: str | None = "Eixample",
    property_type: str = "apartment",
    bedrooms: int = 2,
    avg_nightly_rate: float = 100.0,
    sample_size: int = 50,
    platform: str | None = None,
) -> MarketPrice:
    return MarketPrice(
        event_id=uuid4(),
        market_area=MarketArea(city=city, neighborhood=neighborhood, country_code="ES"),
        property_profile=PropertyProfile(type=property_type, bedrooms=bedrooms),
        target_date=date(2026, 11, 1),
        pricing=Pricing(avg_nightly_rate=avg_nightly_rate),
        market_context=MarketContext(
            sample_size=sample_size, data_source="mock", platform=platform
        ),
        collected_at=datetime(2026, 10, 1, 12, 0, 0),
    )


def test_segment_key_combines_area_type_and_bedrooms():
    event = _market_price(city="Barcelona", neighborhood="Eixample", bedrooms=2)
    assert segment_key(event) == "Barcelona/Eixample|apartment|2"


def test_segment_key_drops_missing_neighborhood():
    event = _market_price(city="Madrid", neighborhood=None)
    assert segment_key(event) == "Madrid|apartment|2"


def test_is_blended_snapshot_true_for_platform_none():
    assert is_blended_snapshot(_market_price(platform=None)) is True


def test_is_blended_snapshot_false_for_channel_specific():
    assert is_blended_snapshot(_market_price(platform="airbnb")) is False


def test_weighted_average_matches_manual_calculation():
    # docs/tech-concepts/windowing-design-checklist.md §3 worked example:
    # (100, 50), (100, 50), (300, 2) -> weighted avg ~= 103.9, not 166.7.
    agg = MarketPulseAggregateFunction()
    acc = agg.create_accumulator()
    for rate, sample_size in [(100.0, 50), (100.0, 50), (300.0, 2)]:
        event = _market_price(avg_nightly_rate=rate, sample_size=sample_size)
        acc = agg.add(event, acc)
    result = agg.get_result(acc)
    assert round(result["avg_price_eur"], 1) == 103.9
    assert result["sample_count"] == 3


def test_min_and_max_price_over_multiple_events():
    agg = MarketPulseAggregateFunction()
    acc = agg.create_accumulator()
    for rate in [100.0, 300.0, 50.0]:
        acc = agg.add(_market_price(avg_nightly_rate=rate, sample_size=1), acc)
    result = agg.get_result(acc)
    assert result["min_price_eur"] == 50.0
    assert result["max_price_eur"] == 300.0


def test_single_event_window_min_equals_max():
    agg = MarketPulseAggregateFunction()
    acc = agg.create_accumulator()
    acc = agg.add(_market_price(avg_nightly_rate=120.0, sample_size=10), acc)
    result = agg.get_result(acc)
    assert result["min_price_eur"] == 120.0
    assert result["max_price_eur"] == 120.0
