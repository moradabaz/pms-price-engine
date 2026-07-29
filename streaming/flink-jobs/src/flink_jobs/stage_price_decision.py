from datetime import UTC, date, datetime
from uuid import uuid4

from pyflink.common.typeinfo import Types
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import KeyedCoProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from shared_schemas.market_price import MarketPrice
from shared_schemas.price_decision import (
    BillingPeriod,
    Calculation,
    CostInputs,
    MarketInputs,
    Output,
    PriceDecision,
)

from flink_jobs.eviction import (
    expired_night_keys,
    is_over_capacity,
    oldest_key_by_updated_at,
)
from flink_jobs.models import CostAggregate, MarketSnapshot
from flink_jobs.pricing import decide_price
from flink_jobs.staleness import is_safe_to_overwrite
from flink_jobs.watchdog import expired_keys, next_deadline_millis

DATA_STALE_TAG = OutputTag("data-stale", Types.PICKLED_BYTE_ARRAY())

APARTMENTS_DESCRIPTOR = MapStateDescriptor(
    "apartments-in-segment", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
NIGHTS_DESCRIPTOR = MapStateDescriptor(
    "nights-in-segment", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
APARTMENT_DEADLINES_DESCRIPTOR = MapStateDescriptor(
    "apartment-deadlines", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
NIGHT_DEADLINES_DESCRIPTOR = MapStateDescriptor(
    "night-deadlines", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)


class PriceDecisionFunction(KeyedCoProcessFunction):
    """Stage B: cross-joins known apartments and known nights within one
    segment, computing a price_decision on every relevant change. Emits
    PriceDecision on the main output, DataStale alerts on a side output."""

    def open(self, runtime_context):
        self.apartments = runtime_context.get_map_state(APARTMENTS_DESCRIPTOR)
        self.nights = runtime_context.get_map_state(NIGHTS_DESCRIPTOR)
        self.apartment_deadlines = runtime_context.get_map_state(
            APARTMENT_DEADLINES_DESCRIPTOR
        )
        self.night_deadlines = runtime_context.get_map_state(NIGHT_DEADLINES_DESCRIPTOR)

    def process_element1(self, value: CostAggregate, ctx):
        """Cost side: updates one apartment, then reprices every known night."""
        existing = self.apartments.get(value.apartment_id)
        if not is_safe_to_overwrite(
            value.updated_at, existing.updated_at if existing else None
        ):
            return

        if is_over_capacity(
            len(dict(self.apartments.items())) + (0 if existing else 1)
        ):
            oldest = oldest_key_by_updated_at(dict(self.apartments.items()))
            self.apartments.remove(oldest)
            self.apartment_deadlines.remove(oldest)

        self.apartments.put(value.apartment_id, value)
        deadline_millis = next_deadline_millis(datetime.now(UTC))
        self.apartment_deadlines.put(value.apartment_id, deadline_millis)
        ctx.timer_service().register_processing_time_timer(deadline_millis)

        for target_date_str, snapshot in dict(self.nights.items()).items():
            yield _build_price_decision(
                value, snapshot, date.fromisoformat(target_date_str)
            )

    def process_element2(self, value: MarketPrice, ctx):
        """Market side: updates one night, then reprices every known apartment."""
        target_date = value.target_date
        if target_date < datetime.now(UTC).date():
            return

        key = target_date.isoformat()
        neighborhood = value.market_area.neighborhood or ""
        market_area = f"{value.market_area.city}/{neighborhood}".rstrip("/")
        snapshot = MarketSnapshot(
            market_area=market_area,
            avg_nightly_rate_eur=value.pricing.avg_nightly_rate,
            occupancy_rate=value.market_context.occupancy_rate,
            sample_size=value.market_context.sample_size,
            collected_at=value.collected_at,
        )
        existing = self.nights.get(key)
        if not is_safe_to_overwrite(
            snapshot.collected_at, existing.collected_at if existing else None
        ):
            return

        expired = expired_night_keys(
            {k: date.fromisoformat(k) for k in dict(self.nights.items())},
            today=datetime.now(UTC).date(),
        )
        for expired_key in expired:
            self.nights.remove(expired_key)
            self.night_deadlines.remove(expired_key)

        self.nights.put(key, snapshot)
        deadline_millis = next_deadline_millis(datetime.now(UTC))
        self.night_deadlines.put(key, deadline_millis)
        ctx.timer_service().register_processing_time_timer(deadline_millis)

        for apartment_id, cost in dict(self.apartments.items()).items():
            yield _build_price_decision(cost, snapshot, target_date)

    def on_timer(self, timestamp: int, ctx):
        """Fires data_stale for apartments/nights whose deadline matches timestamp."""
        for apartment_id in expired_keys(
            dict(self.apartment_deadlines.items()), timestamp
        ):
            yield DATA_STALE_TAG, ("apartment", apartment_id)
        for night_key in expired_keys(dict(self.night_deadlines.items()), timestamp):
            yield DATA_STALE_TAG, ("night", night_key)


def _build_price_decision(
    cost: CostAggregate, market: MarketSnapshot, target_date: date
) -> PriceDecision:
    """Applies the pricing formula and assembles a PriceDecision. Returns it."""
    decided_at = datetime.now(UTC)
    calc = decide_price(
        daily_cost_eur=cost.daily_cost_eur,
        target_margin=cost.target_margin,
        avg_nightly_rate_eur=market.avg_nightly_rate_eur,
        competitiveness_discount=cost.competitiveness_discount,
    )
    return PriceDecision(
        decision_id=uuid4(),
        apartment_id=cost.apartment_id,
        apartment_reference=cost.apartment_reference,
        target_date=target_date,
        decided_at=decided_at,
        cost_inputs=CostInputs(
            billing_period=BillingPeriod(
                start=cost.billing_period_start, end=cost.billing_period_end
            ),
            total_monthly_cost_eur=cost.total_monthly_cost_eur,
            available_days=cost.available_days,
            daily_cost_eur=cost.daily_cost_eur,
            cost_lines_count=cost.cost_lines_count,
        ),
        market_inputs=MarketInputs(
            market_area=market.market_area,
            avg_nightly_rate_eur=market.avg_nightly_rate_eur,
            occupancy_rate=market.occupancy_rate,
            sample_size=market.sample_size,
            collected_at=market.collected_at,
            data_age_seconds=int((decided_at - market.collected_at).total_seconds()),
        ),
        calculation=Calculation(
            target_margin=cost.target_margin,
            minimum_price_eur=calc.minimum_price_eur,
            competitiveness_discount=cost.competitiveness_discount,
            market_reference_price_eur=calc.market_reference_price_eur,
            rule_applied=calc.rule_applied,
        ),
        output=Output(
            suggested_price_eur=calc.suggested_price_eur,
            effective_margin=calc.effective_margin,
            below_market_by=calc.below_market_by,
        ),
    )
