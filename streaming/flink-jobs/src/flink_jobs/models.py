from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class SegmentAssignment:
    """An apartment's segment and pricing config, as stored in broadcast state."""

    city: str
    neighborhood: str
    property_type: str
    bedrooms: int
    target_margin: float
    competitiveness_discount: float


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

    def to_assignment(self) -> SegmentAssignment:
        """Drops apartment_id (used as the map key, not stored in the value)."""
        return SegmentAssignment(
            city=self.city,
            neighborhood=self.neighborhood,
            property_type=self.property_type,
            bedrooms=self.bedrooms,
            target_margin=self.target_margin,
            competitiveness_discount=self.competitiveness_discount,
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
    daily_cost_eur: float
    total_monthly_cost_eur: float
    available_days: int
    cost_lines_count: int
    billing_period_start: date
    billing_period_end: date
    target_margin: float
    competitiveness_discount: float
    updated_at: datetime

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
