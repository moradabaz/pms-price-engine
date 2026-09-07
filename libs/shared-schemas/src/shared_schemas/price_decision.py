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
# Phase 11 (ADR-0011 backlog #5): which revenue base commission_pct is
# charged against.
CommissionBase = Literal[
    "total_revenue", "revenue_minus_ota", "revenue_minus_ota_minus_cleaning"
]
# Phase 12 (ADR-0011 backlog #10): explicit classification of floor_type
# into the external spec's Hard/Soft floor vocabulary.
FloorPolicy = Literal["hard", "soft"]

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
    # Phase 11 (ADR-0011 backlog #5): only on Calculation.decision_components,
    # never on LosFloorCandidate.decision_components (spec 11 §F/AC-06).
    "commission_base_netting",
    # Phase 14 (ADR-0011 backlog #9): only on Calculation.decision_components,
    # never on LosFloorCandidate.decision_components — a human action applied
    # after the whole LOS matrix is computed, not a per-stay-length rule
    # (spec 14 §3).
    "manual_override_applied",
    # Phase 15 (ADR-0011 backlog #12): only on Calculation.decision_components
    # — a recommendation derived from the whole los_floor_matrix, not a
    # per-stay-length rule (spec 15 §3/§4).
    "minimum_stay_recommended",
    "minimum_stay_not_viable",
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


class ManualOverrideDetails(BaseModel):
    """Populated iff an unexpired manual override applied to this decision
    (Phase 14, ADR-0011 backlog #9). Applied by Stage C, after the pricing
    engine (libs/pricing-formulas) has already produced its own suggestion —
    this is a human action layered on top, not a Revenue Management rule."""

    model_config = ConfigDict(extra="forbid")

    override_price_eur: float = Field(ge=0)
    reason: str
    authorized_by: str
    valid_until: datetime
    # max(0, minimum_price_eur - override_price_eur) — zero when the override
    # is at or above the algorithmic floor (spec 14 §4).
    expected_loss_eur: float = Field(ge=0)


class MinimumStayRecommendation(BaseModel):
    """Minimum Stay as a profitability lever (Phase 15, ADR-0011 backlog
    #12). Always present — pure post-processing over los_floor_matrix,
    computed by the same Stage B call that already builds it."""

    model_config = ConfigDict(extra="forbid")

    recommended_min_stay: int | None = Field(default=None, ge=1)
    # 0.0 when recommended_min_stay == 1 (already fine, nothing to relieve);
    # None iff recommended_min_stay is None (no candidate resolves it).
    floor_relief_eur: float | None = Field(default=None, ge=0)
    # Total cost for a reservation of recommended_min_stay nights: fixed/
    # variable cost recur every night, one_time_cost_eur is paid once per
    # booking — the inverse of decide_price()'s per-night amortization. None
    # iff recommended_min_stay is None.
    cost_per_reservation_eur: float | None = Field(default=None, ge=0)
    # The recommended_min_stay LOS candidate's own suggested_price_eur
    # (never itself cost_protected) x its stay_length — a total reservation
    # price already guaranteed to clear the cost floor and sit at/below the
    # market reference. None iff recommended_min_stay is None.
    suggested_price_per_reservation_eur: float | None = Field(default=None, ge=0)


class LosFloorCandidate(BaseModel):
    """One LOS floor matrix entry (ADR-0011 backlog #1) — everything that
    varies with stay_length for an otherwise-identical decision."""

    model_config = ConfigDict(extra="forbid")

    stay_length: int = Field(ge=1)
    minimum_price_eur: float = Field(ge=0)
    floor_type: FloorType
    floor_policy: FloorPolicy
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
    floor_policy: FloorPolicy
    commission_pct: float = Field(ge=0, le=1)
    commission_base: CommissionBase
    days_to_arrival: int
    competitiveness_discount: float = Field(ge=0, le=1)
    property_attribute_factor: float = Field(ge=0)
    property_reference_price_eur: float = Field(ge=0)
    market_reference_price_eur: float = Field(ge=0)
    rule_applied: RuleApplied
    los_floor_matrix: list[LosFloorCandidate] = Field(min_length=1)
    decision_components: list[DecisionComponent] = Field(min_length=1)
    # Phase 14 (ADR-0011 backlog #9): None for the overwhelming majority of
    # decisions (no active override) — never fabricated, never required.
    manual_override: ManualOverrideDetails | None = None
    # Phase 15 (ADR-0011 backlog #12): always present, unlike manual_override
    # — computed from los_floor_matrix, which is itself always present.
    minimum_stay_recommendation: MinimumStayRecommendation


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
