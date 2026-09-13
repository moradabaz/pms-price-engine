from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# Mirrors specs/events/company_cost_occurrence.v1.json field-for-field.


class CompanyCostOccurrence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    company_cost_occurrence_id: UUID
    schema_version: Literal["1.0"] = "1.0"

    cost_definition_id: UUID

    billing_period_start: date
    billing_period_end: date

    amount_gross: float = Field(ge=0)

    description: str

    created_at: datetime
    updated_at: datetime | None = None
