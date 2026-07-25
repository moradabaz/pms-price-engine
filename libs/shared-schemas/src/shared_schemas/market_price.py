from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# Mirrors specs/events/market_price.v1.json field-for-field, including its
# additionalProperties: false at every nesting level (extra="forbid" below).
# Every market_price event is a point-in-time snapshot, not a mutable row —
# event_id is a pure replay-dedup key here, unlike payment_line's event_id
# (see specs/phases/03-market-ingestion/spec.md §3).


class MarketArea(BaseModel):
    model_config = ConfigDict(extra="forbid")

    city: str
    neighborhood: str | None = None
    country_code: str = Field(pattern=r"^[A-Z]{2}$")


class PropertyProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["apartment", "studio", "house", "villa", "room"]
    bedrooms: int = Field(ge=0)
    max_guests: int | None = Field(default=None, ge=1)


class Pricing(BaseModel):
    model_config = ConfigDict(extra="forbid")

    avg_nightly_rate: float = Field(ge=0)
    p25: float | None = Field(default=None, ge=0)
    p50: float | None = Field(default=None, ge=0)
    p75: float | None = Field(default=None, ge=0)
    currency: Literal["EUR"] = "EUR"


class MarketContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    occupancy_rate: float | None = Field(default=None, ge=0, le=1)
    sample_size: int = Field(ge=1)
    data_source: Literal["inside_airbnb", "mock", "scraped_airbnb", "scraped_booking"]
    platform: Literal["airbnb", "booking", "vrbo", "mixed"] | None = None


class MarketPrice(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    schema_version: Literal["1.0"] = "1.0"
    market_area: MarketArea
    property_profile: PropertyProfile
    target_date: date
    pricing: Pricing
    market_context: MarketContext
    collected_at: datetime
