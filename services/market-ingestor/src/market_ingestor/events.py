import random
from datetime import datetime, timedelta
from uuid import uuid4

from shared_schemas.market_price import MarketPrice

from market_ingestor.pricing import sample_occupancy_rate, sample_pricing
from market_ingestor.segments import Segment
from market_ingestor.settings import MarketIngestorSettings


def build_market_price_event(
    segment: Segment,
    settings: MarketIngestorSettings,
    rng: random.Random,
    now: datetime,
) -> MarketPrice:
    pricing, sample_size = sample_pricing(segment, rng)
    target_date = now.date() + timedelta(days=rng.randint(1, settings.forecast_days))

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
