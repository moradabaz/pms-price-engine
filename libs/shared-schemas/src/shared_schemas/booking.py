from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# Mirrors specs/events/booking.v1.json field-for-field.


class Booking(BaseModel):
    model_config = ConfigDict(extra="forbid")

    booking_id: UUID
    schema_version: Literal["1.0"] = "1.0"

    apartment_id: str

    check_in: date
    check_out: date

    channel: Literal["airbnb", "booking", "vrbo", "direct"]

    guests: int = Field(ge=1)
    revenue_eur: float = Field(ge=0)

    status: Literal["confirmed", "cancelled"]

    created_at: datetime
    updated_at: datetime | None = None
