from dataclasses import dataclass, field
from datetime import date, datetime

from pricing_formulas.decision_components import DecisionComponent
from pricing_formulas.layers.commercial import RevenueBase

from flink_jobs.cost_aggregation import (
    ConceptAmount,
    PendingAllocationCorrection,
    PercentageCostComponent,
)


@dataclass(frozen=True)
class SegmentAssignment:
    """An apartment's segment and pricing config, as stored in broadcast state.

    Phase 11 (ADR-0011 backlog #5): commission_pct moved to
    OwnerContractAssignment — this class no longer carries it at all, the
    same real removal (not another additive field) spec 11 §3 documents for
    apartment_market_segments itself."""

    city: str
    neighborhood: str
    property_type: str
    bedrooms: int
    target_margin: float
    competitiveness_discount: float
    # Phase 8 (ADR-0011 backlog #6): raw Property Bonus/Malus attributes.
    # Defaults match apartment_market_segments' own column defaults, for CDC
    # messages predating this phase (same pattern commission_pct established).
    quality_tier: str = "standard"
    rating: float = 4.0
    has_view: bool = False
    has_parking: bool = False


@dataclass(frozen=True)
class ApartmentSegmentRow:
    """One apartment_market_segments CDC row, as received from Kafka."""

    apartment_id: str
    city: str
    neighborhood: str
    property_type: str
    bedrooms: int
    target_margin: float
    competitiveness_discount: float
    quality_tier: str = "standard"
    rating: float = 4.0
    has_view: bool = False
    has_parking: bool = False

    def to_assignment(self) -> SegmentAssignment:
        """Drops apartment_id (used as the map key, not stored in the value)."""
        return SegmentAssignment(
            city=self.city,
            neighborhood=self.neighborhood,
            property_type=self.property_type,
            bedrooms=self.bedrooms,
            target_margin=self.target_margin,
            competitiveness_discount=self.competitiveness_discount,
            quality_tier=self.quality_tier,
            rating=self.rating,
            has_view=self.has_view,
            has_parking=self.has_parking,
        )


@dataclass(frozen=True)
class OwnerContractAssignment:
    """An apartment's commission config, as stored in Stage A2's broadcast
    state (Phase 11, ADR-0011 backlog #5). Phase 20 (ADR-0013 §4): holds only
    the apartment's owner-commission CostDefinition id now — commission_base/
    commission_pct are resolved from that CostDefinition's own
    revenue_base/rate (the same cost_definitions/cost_allocation_rules
    broadcast state Stage A0 already reads), not carried here directly."""

    cost_definition_id: str


@dataclass(frozen=True)
class OwnerContractRow:
    """One owner_contracts CDC row, as received from Kafka."""

    apartment_id: str
    owner_id: str
    cost_definition_id: str

    def to_assignment(self) -> OwnerContractAssignment:
        """Drops apartment_id (used as the map key) and owner_id (unused
        downstream of this table)."""
        return OwnerContractAssignment(cost_definition_id=self.cost_definition_id)


@dataclass(frozen=True)
class ManualOverrideAssignment:
    """An apartment/night's authorized override, as stored in Stage C's
    broadcast state (Phase 14, ADR-0011 backlog #9)."""

    override_price_eur: float
    reason: str
    authorized_by: str
    valid_until: datetime


@dataclass(frozen=True)
class ManualOverrideRow:
    """One manual_overrides CDC row, as received from Kafka."""

    apartment_id: str
    target_date: date
    override_price_eur: float
    reason: str
    authorized_by: str
    valid_until: datetime

    def broadcast_key(self) -> str:
        """The composite key ManualOverrideEnrichmentFunction's broadcast
        state uses — apartment_id/target_date are the map key, not stored in
        the value (same drop-the-key convention OwnerContractRow.to_assignment
        established). Returns the key."""
        return f"{self.apartment_id}:{self.target_date.isoformat()}"

    def to_assignment(self) -> ManualOverrideAssignment:
        return ManualOverrideAssignment(
            override_price_eur=self.override_price_eur,
            reason=self.reason,
            authorized_by=self.authorized_by,
            valid_until=self.valid_until,
        )


@dataclass(frozen=True)
class BookingRow:
    """One bookings CDC row, as received from Kafka (Phase 18, ADR-0011
    backlog #13 prerequisite)."""

    booking_id: str
    apartment_id: str
    check_in: date
    check_out: date
    channel: str
    guests: int
    revenue_eur: float
    status: str


@dataclass(frozen=True)
class CostDefinitionRow:
    """One cost_definitions CDC row, as received from Kafka (Phase 19,
    ADR-0011 backlog #13, ADR-0012)."""

    cost_definition_id: str
    concept: str
    scope: str
    behavior: str
    trigger: str
    calculation_base: str
    recurrence: str
    revenue_base: str | None
    # Phase 20 (ADR-0013 §3): only meaningful when calculation_base is
    # percentage-based.
    rate: float | None = None


@dataclass(frozen=True)
class CostAllocationRuleRow:
    """One cost_allocation_rules CDC row, as received from Kafka (Phase 19)."""

    cost_definition_id: str
    method: str
    weight_config: str | None


@dataclass(frozen=True)
class CompanyCostOccurrenceRow:
    """One company_cost_occurrences CDC row, as received from Kafka (Phase
    19) — a company-scoped cost's real amount for one period, broadcast to
    every apartment (spec 19 §5)."""

    company_cost_occurrence_id: str
    cost_definition_id: str
    billing_period_start: date
    billing_period_end: date
    amount_gross: float


@dataclass(frozen=True)
class CostAggregate:
    """Stage A's output: one apartment's current cost, segment, and margin config."""

    apartment_id: str
    apartment_reference: str
    city: str
    neighborhood: str
    property_type: str
    bedrooms: int
    fixed_cost_eur: float
    variable_cost_eur: float
    # Renamed from one_time_cost_eur (Phase 20, ADR-0013) — same "average
    # per turnover, divided by the candidate's own stay_length inside
    # decide_price()" semantics, unchanged mechanically.
    per_booking_cost_eur: float
    total_monthly_cost_eur: float
    available_days: int
    cost_lines_count: int
    billing_period_start: date
    billing_period_end: date
    target_margin: float
    competitiveness_discount: float
    updated_at: datetime
    # Phase 8 (ADR-0011 backlog #6): resolved once in Stage A from the
    # apartment's raw Bonus/Malus attributes (property_attributes.py) — Stage B
    # and decide_price() consume this single float, never the raw attributes.
    # Default 1.0 (neutral) keeps every pre-Phase-8 CostAggregate construction
    # (tests included) valid without change.
    property_attribute_factor: float = 1.0
    # Phase 10 (ADR-0011 backlog #4): the four attribute contributions behind
    # property_attribute_factor, resolved once in Stage A alongside it.
    property_decision_components: tuple[DecisionComponent, ...] = field(
        default_factory=tuple
    )
    # Phase 11 (ADR-0011 backlog #5): resolved by Stage A2
    # (OwnerContractEnrichmentFunction), not Stage A — these two defaults are
    # only ever observed on a CostAggregate that hasn't reached Stage A2 yet
    # (e.g. in a test constructing one directly); every CostAggregate Stage B
    # actually sees has already passed through Stage A2's broadcast join.
    # 0.15/"total_revenue" match this project's pre-Phase-20 defaults. Phase
    # 20 (ADR-0013 §4): sourced from the apartment's own owner-commission
    # CostDefinition (rate/revenue_base) instead of owner_contracts' own
    # columns directly — the field names/roles are otherwise unchanged.
    commission_pct: float = 0.15
    commission_base: RevenueBase = "total_revenue"
    # Phase 20 (ADR-0013 §3): every OTHER applicable percentage CostDefinition
    # for this apartment/period (payment-line-derived, e.g. ota_fee) —
    # resolved once in Stage A from cost_aggregation.py. The owner-commission
    # CostDefinition (commission_pct/commission_base above) is NOT included
    # here (it is resolved separately, by Stage A2) — stage_price_decision.py
    # combines both into the formula's own `p`.
    percentage_costs: tuple[PercentageCostComponent, ...] = field(
        default_factory=tuple
    )
    # Phase 11: resolved once in Stage A from cost_aggregation.py's new
    # concept-based sub-totals — already fully counted inside
    # fixed_cost_eur/variable_cost_eur above, these are additional
    # breakdowns for decide_price()'s revenue-base netting, not new costs.
    ota_related_cost_eur: float = 0.0
    cleaning_cost_eur: float = 0.0
    # Phase 17 (ADR-0011 backlog #3): resolved once in Stage A alongside
    # ota_related_cost_eur/cleaning_cost_eur above, from the same
    # cost_aggregation.py computation — every concept observed in the
    # current billing period, not just those two special-cased groups.
    cost_breakdown: tuple[ConceptAmount, ...] = field(default_factory=tuple)
    # Phase 18 (ADR-0011 backlog #13 prerequisite): resolved by the new
    # booking-enrichment stage, chained right after Stage A. Default 0 until
    # that stage has seen at least one booking event for this apartment —
    # "available nights" is not a separate field yet, it is available_days
    # above (no apartment-blocked/maintenance concept exists in this PoC).
    occupied_nights: int = 0
    # Phase 19 (ADR-0011 backlog #13): same stage as occupied_nights above,
    # counts distinct confirmed bookings overlapping the current billing
    # period — needed by the 'booking' allocation method.
    booking_count: int = 0
    # Phase 20 (ADR-0013 §2): same stage as occupied_nights/booking_count
    # above — average guests per confirmed, overlapping booking, needed by
    # calculation_base='per_guest'.
    avg_guests: float = 0.0
    # Phase 19: concepts whose allocation_method needs occupied_nights/
    # booking_count (not yet available when Stage A runs) — resolved by
    # Stage A-correction, chained after the booking stage. Empty once
    # corrected (the correction folds its amount into fixed_cost_eur/
    # variable_cost_eur and clears this tuple, so it never applies twice).
    pending_allocation_corrections: tuple[PendingAllocationCorrection, ...] = field(
        default_factory=tuple
    )

    @property
    def segment_key(self) -> tuple[str, str, str, int]:
        """Returns the (city, neighborhood, property_type, bedrooms) segment key."""
        return (self.city, self.neighborhood, self.property_type, self.bedrooms)


@dataclass(frozen=True)
class MarketSnapshot:
    """One segment's known market price for a single target_date — either
    the blended (channel-agnostic) rate, or one specific channel's own rate
    (Phase 16, ADR-0011 backlog #2), depending on which map a NightSnapshot
    holds it in."""

    market_area: str
    avg_nightly_rate_eur: float
    occupancy_rate: float | None
    sample_size: int | None
    collected_at: datetime


@dataclass(frozen=True)
class NightSnapshot:
    """Everything Stage B knows about one segment's single target_date
    (Phase 16, ADR-0011 backlog #2): the blended snapshot Stage B has always
    tracked, plus whichever per-channel snapshots have arrived so far.
    `blended` is `None` only transiently — a channel-only event for a
    brand-new night is stored but doesn't fan out (same "known night"
    definition Stage B already used pre-Phase-16); in practice this window
    is one Kafka round-trip, since market-ingestor emits the blended event
    and every channel event for a segment/tick together."""

    blended: MarketSnapshot | None
    channels: dict[str, MarketSnapshot] = field(default_factory=dict)

    def channel_rates_eur(self) -> dict[str, float]:
        """Returns {platform: avg_nightly_rate_eur} for decide_price_by_
        channel() — the only part of each channel MarketSnapshot it needs."""
        return {
            platform: snapshot.avg_nightly_rate_eur
            for platform, snapshot in self.channels.items()
        }
