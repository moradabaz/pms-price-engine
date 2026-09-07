import random
from datetime import UTC, date, datetime

from market_ingestor.events import build_channel_market_price_events
from market_ingestor.pricing import CHANNEL_PRICE_MULTIPLIERS, sample_channel_pricing
from market_ingestor.segments import SEGMENTS


def test_sample_channel_pricing_is_statistically_coherent():
    # Same coherence guarantee sample_pricing() gives (p25 <= p50 <= p75,
    # derived from the same draw, never generated independently).
    segment = SEGMENTS[0]
    rng = random.Random(42)

    pricing, sample_size = sample_channel_pricing(
        segment, "airbnb", date(2026, 8, 15), rng
    )

    assert pricing["p25"] <= pricing["p50"] <= pricing["p75"]
    assert pricing["avg_nightly_rate"] > 0
    assert 3 <= sample_size <= 45
    assert pricing["currency"] == "EUR"


def test_sample_channel_pricing_applies_the_platform_multiplier():
    # Airbnb's multiplier (1.08) is above Vrbo's (0.92) — across many draws
    # with the same seed stream, Airbnb's average should land higher.
    segment = SEGMENTS[0]
    target_date = date(2026, 8, 15)

    airbnb_avgs = [
        sample_channel_pricing(segment, "airbnb", target_date, random.Random(seed))[
            0
        ]["avg_nightly_rate"]
        for seed in range(20)
    ]
    vrbo_avgs = [
        sample_channel_pricing(segment, "vrbo", target_date, random.Random(seed))[0][
            "avg_nightly_rate"
        ]
        for seed in range(20)
    ]

    assert sum(airbnb_avgs) > sum(vrbo_avgs)


def test_build_channel_market_price_events_one_per_platform():
    segment = SEGMENTS[0]
    rng = random.Random(1)
    now = datetime.now(UTC)

    events = build_channel_market_price_events(segment, date(2026, 8, 15), rng, now)

    assert len(events) == len(CHANNEL_PRICE_MULTIPLIERS)
    platforms = {event.market_context.platform for event in events}
    assert platforms == set(CHANNEL_PRICE_MULTIPLIERS)
    for event in events:
        assert event.market_context.data_source == "mock"
        assert event.pricing.avg_nightly_rate > 0
