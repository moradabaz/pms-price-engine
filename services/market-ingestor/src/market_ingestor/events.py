import random
from datetime import date, datetime
from uuid import uuid4

from shared_schemas.market_price import MarketPrice

from market_ingestor.pricing import sample_occupancy_rate, sample_pricing
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
