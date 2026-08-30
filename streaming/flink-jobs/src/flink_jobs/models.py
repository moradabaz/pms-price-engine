from dataclasses import dataclass, field
from datetime import date, datetime

from flink_jobs.decision_components import DecisionComponent
from flink_jobs.pricing import CommissionBase


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
    state (Phase 11, ADR-0011 backlog #5)."""

    commission_base: CommissionBase
    commission_pct: float


@dataclass(frozen=True)
class OwnerContractRow:
    """One owner_contracts CDC row, as received from Kafka."""

    apartment_id: str
    owner_id: str
    commission_base: CommissionBase
    commission_pct: float

    def to_assignment(self) -> OwnerContractAssignment:
        """Drops apartment_id (used as the map key) and owner_id (unused
        downstream of this table — decide_price() only needs the resolved
        commission_base/commission_pct pair, spec 11 §2)."""
        return OwnerContractAssignment(
            commission_base=self.commission_base,
            commission_pct=self.commission_pct,
        )


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
    one_time_cost_eur: float
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
    # 0.15/"total_revenue" match owner_contracts' own column defaults.
    commission_pct: float = 0.15
    commission_base: CommissionBase = "total_revenue"
    # Phase 11: resolved once in Stage A from cost_aggregation.py's new
    # concept-based sub-totals — already fully counted inside
    # fixed_cost_eur/variable_cost_eur above, these are additional
    # breakdowns for decide_price()'s commission-base netting, not new costs.
    ota_related_cost_eur: float = 0.0
    cleaning_cost_eur: float = 0.0

    @property
    def segment_key(self) -> tuple[str, str, str, int]:
        """Returns the (city, neighborhood, property_type, bedrooms) segment key."""
        return (self.city, self.neighborhood, self.property_type, self.bedrooms)


@dataclass(frozen=True)
class MarketSnapshot:
    """One segment's known market price for a single target_date."""

    market_area: str
    avg_nightly_rate_eur: float
    occupancy_rate: float | None
    sample_size: int | None
    collected_at: datetime
