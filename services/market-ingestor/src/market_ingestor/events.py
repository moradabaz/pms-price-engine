import random
from datetime import date, datetime
from uuid import uuid4

from shared_schemas.market_price import MarketPrice

from market_ingestor.pricing import (
    CHANNEL_PRICE_MULTIPLIERS,
    sample_channel_pricing,
    sample_occupancy_rate,
    sample_pricing,
)
from market_ingestor.segments import Segment


def build_market_price_event(
    segment: Segment,
    target_date: date,
    rng: random.Random,
    now: datetime,
) -> MarketPrice:
    # target_date is computed once per tick by the caller (main.run_tick),
    # shared by every segment — Decision D.1's deterministic cyclic coverage,
    # not a per-segment random pick anymore.
    pricing, sample_size = sample_pricing(segment, target_date, rng)

    return MarketPrice.model_validate(
        {
            "event_id": str(uuid4()),
            "schema_version": "1.0",
            "market_area": {
                "city": segment.city,
                "neighborhood": segment.neighborhood,
                "country_code": segment.country_code,
            },
            "property_profile": {
                "type": segment.property_type,
                "bedrooms": segment.bedrooms,
                "max_guests": segment.max_guests,
            },
            "target_date": target_date.isoformat(),
            "pricing": pricing,
            "market_context": {
                "occupancy_rate": sample_occupancy_rate(rng),
                "sample_size": sample_size,
                "data_source": "mock",
                "platform": None,
            },
            "collected_at": now.isoformat(),
        }
    )


def build_channel_market_price_events(
    segment: Segment,
    target_date: date,
    rng: random.Random,
    now: datetime,
) -> list[MarketPrice]:
    """One additional MarketPrice per known channel (ADR-0011 backlog #2),
    alongside — never instead of — build_market_price_event()'s existing
    blended one. Same shape, market_context.platform set to a real value
    and data_source unchanged (still "mock"). Returns one event per
    CHANNEL_PRICE_MULTIPLIERS key."""
    events = []
    for platform in CHANNEL_PRICE_MULTIPLIERS:
        pricing, sample_size = sample_channel_pricing(
            segment, platform, target_date, rng
        )
        events.append(
            MarketPrice.model_validate(
                {
                    "event_id": str(uuid4()),
                    "schema_version": "1.0",
                    "market_area": {
                        "city": segment.city,
                        "neighborhood": segment.neighborhood,
                        "country_code": segment.country_code,
                    },
                    "property_profile": {
                        "type": segment.property_type,
                        "bedrooms": segment.bedrooms,
                        "max_guests": segment.max_guests,
                    },
                    "target_date": target_date.isoformat(),
                    "pricing": pricing,
                    "market_context": {
                        "occupancy_rate": sample_occupancy_rate(rng),
                        "sample_size": sample_size,
                        "data_source": "mock",
                        "platform": platform,
                    },
                    "collected_at": now.isoformat(),
                }
            )
        )
    return events
