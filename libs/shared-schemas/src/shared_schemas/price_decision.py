from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# Mirrors specs/events/price_decision.v1.json field-for-field.

# Shared between Calculation and LosFloorCandidate (Phase 9) — defined once
# rather than repeating the same enum tuple in two Pydantic models.
FloorType = Literal[
    "structural_full_margin", "structural_reduced_margin", "contribution"
]
RuleApplied = Literal["market_competitive", "minimum_floor", "cost_protected"]

# Phase 10 (ADR-0011 backlog #4): closed reason-code vocabulary shared by
# Calculation.decision_components and LosFloorCandidate.decision_components.
ReasonCode = Literal[
    "property_quality_tier",
    "property_rating",
    "property_view",
    "property_parking",
    "rule_market_competitive",
    "rule_minimum_floor",
    "rule_cost_protected",
]


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


class DecisionComponent(BaseModel):
    """One structured reason code explaining part of a pricing decision
    (Phase 10, ADR-0011 backlog #4). impact's unit is contextual to code's
    family: a signed adjustment fraction for property_* codes, a signed EUR
    gap for rule_* codes."""

    model_config = ConfigDict(extra="forbid")

    code: ReasonCode
    label: str
    impact: float


class LosFloorCandidate(BaseModel):
    """One LOS floor matrix entry (ADR-0011 backlog #1) — everything that
    varies with stay_length for an otherwise-identical decision."""

    model_config = ConfigDict(extra="forbid")

    stay_length: int = Field(ge=1)
    minimum_price_eur: float = Field(ge=0)
    floor_type: FloorType
    rule_applied: RuleApplied
    suggested_price_eur: float = Field(ge=0)
    effective_margin: float
    # Phase 10: only this candidate's own rule component — never the
    # property Bonus/Malus block, which doesn't vary with stay_length and
    # already lives once on the top-level Calculation (spec 10 §E).
    decision_components: list[DecisionComponent] = Field(min_length=1)


class Calculation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_margin: float = Field(ge=0)
    minimum_price_eur: float = Field(ge=0)
    floor_type: FloorType
    commission_pct: float = Field(ge=0, le=1)
    days_to_arrival: int
    competitiveness_discount: float = Field(ge=0, le=1)
    property_attribute_factor: float = Field(ge=0)
    property_reference_price_eur: float = Field(ge=0)
    market_reference_price_eur: float = Field(ge=0)
    rule_applied: RuleApplied
    los_floor_matrix: list[LosFloorCandidate] = Field(min_length=1)
    decision_components: list[DecisionComponent] = Field(min_length=1)


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
