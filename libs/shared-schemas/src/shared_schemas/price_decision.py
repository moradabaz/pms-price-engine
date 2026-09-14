from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# Mirrors specs/events/price_decision.v1.json field-for-field.

# Renamed 2026-09-12 (ex-"cost_protected"): the cost floor exceeded the
# property's own market reference, so the floor is charged instead of a
# market-derived price — this protects profitability, it does not mean the
# price fell BELOW cost (see docs/profitable-pricing-glossary.md §5's
# "Profitable Floor" — the same concept this value names).
RuleApplied = Literal["market_competitive", "minimum_floor", "minimum_profitable_price"]
# Phase 23 (ADR-0016 §3): external spec §13's situation/action table as an
# explicit classification. Mirrors pricing_formulas.viability.ViabilityStatus
# field-for-field (shared_schemas has no dependency on pricing_formulas, so
# this is a duplicated Literal, not an import — same convention every other
# closed vocabulary in this file already follows).
ViabilityStatus = Literal[
    "ok",
    "demand_upside",
    "min_stay_lever_available",
    "channel_lever_available",
    "persistent_floor_breach",
    "override_active",
    "floor_binding",
]
# Phase 11 (ADR-0011 backlog #5): which revenue base commission_pct is
# charged against. Phase 20 (ADR-0013 §4): still meaningful — now the
# resolved revenue_base of the apartment's own owner-commission
# CostDefinition, rather than a dedicated owner_contracts column.
# Phase 24 (ADR-0017 §1/§4): the remaining 2 of the external spec's 5 bases
# (§11.1) — additive enum widening, no schema_version bump (same reasoning
# Phase 22's ReasonCode extension already documents).
CommissionBase = Literal[
    "total_revenue",
    "revenue_minus_ota",
    "revenue_minus_ota_minus_cleaning",
    "revenue_minus_ota_minus_cleaning_minus_laundry",
    "revenue_minus_all_booking_costs",
]
# Phase 12 (ADR-0011 backlog #10): explicit classification of the floor into
# the external spec's Hard/Soft floor vocabulary. Phase 20 (ADR-0013 §5):
# no longer derived from an antelación-tiered floor_type (retired) — "hard"
# iff target_margin == 0 (BER and MPR coincide), "soft" otherwise.
FloorPolicy = Literal["hard", "soft"]
# Phase 17 (ADR-0011 backlog #3): mirrors payment_line.v1's concept enum
# field-for-field. Phase 20 (ADR-0013 §4): 'owner_commission' added — the
# owner-contract commission is now a CostDefinition like any other concept.
CostConcept = Literal[
    "electricity",
    "water",
    "gas",
    "internet",
    "pms_subscription",
    "ota_fee",
    "channel_manager",
    "office_rent",
    "cleaning",
    "maintenance",
    "insurance",
    "community_fee",
    "other",
    "owner_commission",
    # Phase 24 (ADR-0017 §3): the 4th/5th revenue bases (§11.1) need a
    # laundry-only sub-total to net against.
    "laundry",
]
# Phase 20 (ADR-0013, spec 20 §2): mirrors cost_definitions.sql's own
# dimension enums field-for-field — cost_breakdown entries carry these
# alongside concept/amount_eur now, informational/display only.
CostScope = Literal["booking", "property", "company"]
CostBehavior = Literal["fixed", "variable", "semi_variable"]
CostTrigger = Literal["reservation", "night", "guest", "time", "revenue", "event"]
CostCalculationBase = Literal[
    "fixed_amount", "pct_revenue", "pct_adjusted_revenue", "per_night", "per_guest"
]
CostRecurrence = Literal[
    "per_booking", "daily", "monthly", "quarterly", "annual", "one_off"
]
CostAllocationMethod = Literal[
    "direct",
    "calendar_day",
    "available_night",
    "occupied_night",
    "booking",
    "revenue",
    "weighted",
]

# Phase 10 (ADR-0011 backlog #4): closed reason-code vocabulary shared by
# Calculation.decision_components and LosFloorCandidate.decision_components.
ReasonCode = Literal[
    "property_quality_tier",
    "property_rating",
    "property_view",
    "property_parking",
    "rule_market_competitive",
    "rule_minimum_floor",
    "rule_minimum_profitable_price",
    # Phase 22 (ADR-0015, spec 22 §3): booking_window_component() — never
    # "rule_standard_window", which is never emitted (a zero-adjustment tier
    # explains nothing), so it is deliberately excluded from this closed
    # vocabulary.
    "rule_early_bird",
    "rule_last_minute",
    "rule_same_day",
    # Phase 11 (ADR-0011 backlog #5), generalized by Phase 20 (ADR-0013 §3)
    # from commission-only to any percentage CostDefinition with a
    # revenue_base: only on Calculation.decision_components, never on
    # LosFloorCandidate.decision_components (spec 11 §F/AC-06).
    "revenue_base_netting",
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


class CostConceptAmount(BaseModel):
    """One payment_line.concept's own per-day cost for the current billing
    period (Phase 17, ADR-0011 backlog #3) — already fully counted inside
    fixed_cost_eur/variable_cost_eur/fixed_and_allocated_costs_eur above,
    same "additional breakdown, not a new cost" convention Phase 11's
    ota_related_cost_eur/cleaning_cost_eur established. Only concepts with
    at least one matching line in the period appear — a concept absent from
    the period has no entry, never a fabricated zero one. Phase 20
    (ADR-0013, spec 20 §2): gains the CostDefinition's own 6 dimensions —
    display-only, does not feed the formula (that's fixed_and_allocated_
    costs_eur/per_booking_cost_eur/p on CostInputs below)."""

    model_config = ConfigDict(extra="forbid")

    concept: CostConcept
    amount_eur: float = Field(ge=0)
    scope: CostScope
    behavior: CostBehavior
    trigger: CostTrigger
    calculation_base: CostCalculationBase
    recurrence: CostRecurrence
    allocation_method: CostAllocationMethod


class CostInputs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    billing_period: BillingPeriod
    total_monthly_cost_eur: float = Field(ge=0)
    available_days: int = Field(ge=1)
    # Informational only from Phase 20 onward (ADR-0013) — grouped by
    # CostDefinition.behavior, no longer read by decide_price() directly.
    fixed_cost_eur: float = Field(ge=0)
    variable_cost_eur: float = Field(ge=0)
    # Phase 20 (ADR-0013): the two terms the Break-Even/Profitable Floor
    # formula (external spec §11) actually consumes.
    fixed_and_allocated_costs_eur: float = Field(ge=0)
    # Renamed from one_time_cost_eur — allocation_method='booking' lines,
    # still a per-turnover average divided by the candidate's own
    # stay_length inside decide_price() (unchanged mechanically).
    per_booking_cost_eur: float = Field(ge=0)
    # Sum of every applicable CostDefinition.rate for this apartment/period,
    # including the migrated owner-commission CostDefinition (ADR-0013 §4).
    p: float = Field(ge=0, le=1)
    cost_lines_count: int | None = Field(default=None, ge=0)
    # Phase 17 (ADR-0011 backlog #3): always present but legitimately can be
    # empty ([]) — same "required list, sparse content" convention Phase 16's
    # channel_price_matrix established, not the "always min_length=1"
    # convention los_floor_matrix/decision_components use.
    cost_breakdown: list[CostConceptAmount] = Field(default_factory=list)


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
    # (never itself minimum_profitable_price) x its stay_length — a total reservation
    # price already guaranteed to clear the cost floor and sit at/below the
    # market reference. None iff recommended_min_stay is None.
    suggested_price_per_reservation_eur: float | None = Field(default=None, ge=0)


class ChannelPriceCandidate(BaseModel):
    """One channel-price matrix entry (Phase 16, ADR-0011 backlog #2) — this
    channel's own market rate and commission, run through the same layered
    engine as the top-level (blended) calculation. Unlike LosFloorCandidate,
    market_reference_price_eur genuinely varies per entry here (each channel
    has its own real avg_nightly_rate_eur, spec 16 §4)."""

    model_config = ConfigDict(extra="forbid")

    platform: Literal["airbnb", "booking", "vrbo"]
    avg_nightly_rate_eur: float = Field(ge=0)
    commission_pct: float = Field(ge=0, le=1)
    market_reference_price_eur: float = Field(ge=0)
    minimum_price_eur: float = Field(ge=0)
    floor_policy: FloorPolicy
    rule_applied: RuleApplied
    suggested_price_eur: float = Field(ge=0)
    effective_margin: float
    # Phase 10: only this candidate's own rule component — same "no property/
    # commission block repeated per candidate" convention LosFloorCandidate
    # already established.
    decision_components: list[DecisionComponent] = Field(min_length=1)


class LosFloorCandidate(BaseModel):
    """One LOS floor matrix entry (ADR-0011 backlog #1) — everything that
    varies with stay_length for an otherwise-identical decision."""

    model_config = ConfigDict(extra="forbid")

    stay_length: int = Field(ge=1)
    minimum_price_eur: float = Field(ge=0)
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
    # Phase 20 (ADR-0013 §11): informational, explainability-only (external
    # spec §14) — never itself substituted as the enforced floor
    # (minimum_price_eur, always MPR/profitable_floor_eur, plays that role).
    break_even_revenue_eur: float = Field(ge=0)
    profitable_floor_eur: float = Field(ge=0)
    floor_policy: FloorPolicy
    commission_pct: float = Field(ge=0, le=1)
    commission_base: CommissionBase
    # Phase 20 (ADR-0013 §5): no longer read by decide_price()'s floor math
    # (there are no antelación tiers left to select) — still recorded here
    # directly by Flink, informational/audit only.
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
    # Phase 16 (ADR-0011 backlog #2): always present but legitimately can be
    # empty ([]) — unlike los_floor_matrix, a channel's candidate only exists
    # once market-ingestor's channel-specific event for that night has
    # arrived, so no minimum length is enforced.
    channel_price_matrix: list[ChannelPriceCandidate] = Field(default_factory=list)
    # Phase 23 (ADR-0016 §2): external spec §13's situation/action
    # classification — required, breaking price_decision.v1 bump to 3.0
    # (same class of change ADR-0012/ADR-0013 already established for their
    # own required-field additions under this project's strict
    # extra="forbid" schema convention).
    viability_status: ViabilityStatus
    # Days this (apartment, target_date) has held rule_applied !=
    # "market_competitive" in an unbroken streak, 0 when currently
    # market_competitive (ADR-0016 §4). Informational/audit — only
    # viability_status is the decision surface.
    floor_breach_days: int = Field(ge=0)
    # Phase 25 (ADR-0018 §2): which insert-only pricing_strategies row
    # (target_margin/competitiveness_discount) produced this decision — the
    # field "reproducibility" (external spec §28) hinges on, since this
    # project does no historical replay (future-only, same precedent
    # ADR-0011/ADR-0013 already established).
    pricing_strategy_version: int = Field(ge=1)


class Output(BaseModel):
    model_config = ConfigDict(extra="forbid")

    suggested_price_eur: float = Field(ge=0)
    currency: Literal["EUR"] = "EUR"
    effective_margin: float
    below_market_by: float


class PriceDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision_id: UUID
    # Phase 20 (ADR-0013 §8): breaking bump — floor_type removed,
    # fixed_and_allocated_costs_eur/per_booking_cost_eur/p/
    # break_even_revenue_eur/profitable_floor_eur added, same class of
    # change as payment_line.v1's 1.0 -> 2.0 (ADR-0012).
    # Phase 23 (ADR-0016 §2): breaking bump — viability_status/
    # floor_breach_days added as required fields on Calculation.
    # Phase 25 (ADR-0018 §2): breaking bump — pricing_strategy_version added
    # as a required field on Calculation.
    schema_version: Literal["4.0"] = "4.0"
    apartment_id: str
    apartment_reference: str
    target_date: date
    decided_at: datetime
    cost_inputs: CostInputs
    market_inputs: MarketInputs
    calculation: Calculation
    output: Output
