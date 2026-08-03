from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# Mirrors specs/events/price_decision.v1.json field-for-field.


class BillingPeriod(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: date
    end: date


class CostInputs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    billing_period: BillingPeriod
    total_monthly_cost_eur: float = Field(ge=0)
    available_days: int = Field(ge=1)
    fixed_cost_eur: float = Field(ge=0)
    variable_cost_eur: float = Field(ge=0)
    one_time_cost_eur: float = Field(ge=0)
    cost_lines_count: int | None = Field(default=None, ge=0)


class MarketInputs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market_area: str
    avg_nightly_rate_eur: float = Field(ge=0)
    occupancy_rate: float | None = Field(default=None, ge=0, le=1)
    sample_size: int | None = Field(default=None, ge=1)
    collected_at: datetime
    data_age_seconds: int = Field(ge=0)


class Calculation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_margin: float = Field(ge=0)
    minimum_price_eur: float = Field(ge=0)
    floor_type: Literal[
        "structural_full_margin", "structural_reduced_margin", "contribution"
    ]
    commission_pct: float = Field(ge=0, le=1)
    days_to_arrival: int
    competitiveness_discount: float = Field(ge=0, le=1)
    market_reference_price_eur: float = Field(ge=0)
    rule_applied: Literal["market_competitive", "minimum_floor", "cost_protected"]


class Output(BaseModel):
    model_config = ConfigDict(extra="forbid")

    suggested_price_eur: float = Field(ge=0)
    currency: Literal["EUR"] = "EUR"
    effective_margin: float
    below_market_by: float


class PriceDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_id: UUID
    schema_version: Literal["1.0"] = "1.0"
    apartment_id: str
    apartment_reference: str
    target_date: date
    decided_at: datetime
    cost_inputs: CostInputs
    market_inputs: MarketInputs
    calculation: Calculation
    output: Output
