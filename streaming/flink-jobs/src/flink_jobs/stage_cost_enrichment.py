from datetime import UTC, datetime

from pricing_formulas.layers.structural import (
    property_attribute_components,
    property_attribute_factor,
)
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedBroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor

from flink_jobs.cost_aggregation import (
    EnrichedPaymentLine,
    aggregate_cost,
    retained_billing_period_ends,
)
from flink_jobs.models import ApartmentSegmentRow, CostAggregate, PricingStrategyRow

# Phase 21 (ADR-0014): broadcast value is a PropertyPricingProfile.
# Phase 25 (ADR-0018 §2): no longer a (PropertyPricingProfile, PricingStrategy)
# tuple — PricingStrategy now comes from its own pricing_strategies CDC
# stream (PRICING_STRATEGY_BROADCAST_DESCRIPTOR below), unioned with this
# descriptor's own source stream in job.py (same "one connected broadcast
# stream, several independently-keyed descriptors" pattern
# stage_owner_contract_enrichment.py already established for
# owner_contracts/cost_definitions) — PyFlink's KeyedStream.connect() accepts
# exactly one broadcast stream per process() call, so a second, genuinely
# independent CDC source cannot be a second .connect() on this same stage.
SEGMENT_BROADCAST_DESCRIPTOR = MapStateDescriptor(
    "apartment-segment-assignments", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
# Phase 25 (ADR-0018 §2): keyed by apartment_id, holding only the highest
# `version` PricingStrategyRow seen so far — CDC delivery order across Kafka
# partitions isn't globally guaranteed, so process_broadcast_element below
# compares versions rather than trusting arrival order (spec 25 §4 AC-03).
PRICING_STRATEGY_BROADCAST_DESCRIPTOR = MapStateDescriptor(
    "pricing-strategy-assignments", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
COST_LINES_STATE_DESCRIPTOR = MapStateDescriptor(
    "payment-lines-by-event-id", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)


class CostEnrichmentFunction(KeyedBroadcastProcessFunction):
    """Stage A: aggregates cost per apartment and enriches it with segment
    and margin config from broadcast state. Emits CostAggregate. Phase 19
    (ADR-0011 backlog #13): consumes EnrichedPaymentLine (Stage A0's output),
    never PaymentLine directly — concept/behavior/etc. are already resolved
    by then."""

    def open(self, runtime_context):
        self.cost_lines_state = runtime_context.get_map_state(
            COST_LINES_STATE_DESCRIPTOR
        )

    def process_element(self, value: EnrichedPaymentLine, ctx):
        self.cost_lines_state.put(value.event_id, value)

        current = dict(self.cost_lines_state.items())
        retained_ends = retained_billing_period_ends(
            line.billing_period_end for line in current.values()
        )
        for event_id, line in current.items():
            if line.billing_period_end not in retained_ends:
                self.cost_lines_state.remove(event_id)

        aggregation = aggregate_cost(dict(self.cost_lines_state.items()).values())
        if aggregation is None:
            return

        profile = ctx.get_broadcast_state(SEGMENT_BROADCAST_DESCRIPTOR).get(
            value.apartment_id
        )
        strategy = ctx.get_broadcast_state(PRICING_STRATEGY_BROADCAST_DESCRIPTOR).get(
            value.apartment_id
        )
        if profile is None or strategy is None:
            # Segment and/or strategy not delivered yet — skip, do not
            # buffer (spec §6; extended by Phase 25, ADR-0018 §2, to the
            # now-independent pricing_strategies stream).
            return

        yield CostAggregate(
            apartment_id=value.apartment_id,
            apartment_reference=value.apartment_reference,
            city=profile.city,
            neighborhood=profile.neighborhood,
            property_type=profile.property_type,
            bedrooms=profile.bedrooms,
            fixed_cost_eur=aggregation.fixed_cost_eur,
            variable_cost_eur=aggregation.variable_cost_eur,
            per_booking_cost_eur=aggregation.per_booking_cost_eur,
            percentage_costs=aggregation.percentage_costs,
            total_monthly_cost_eur=aggregation.total_monthly_cost_eur,
            available_days=aggregation.available_days,
            cost_lines_count=aggregation.cost_lines_count,
            billing_period_start=aggregation.billing_period_start,
            billing_period_end=aggregation.billing_period_end,
            target_margin=strategy.target_margin,
            competitiveness_discount=strategy.competitiveness_discount,
            pricing_strategy_version=strategy.version,
            updated_at=datetime.now(UTC),
            # Phase 11 (ADR-0011 backlog #5): commission_pct/commission_base
            # are no longer resolved here — PricingStrategy doesn't carry
            # commission_pct any more (spec 11 §3). CostAggregate's own
            # defaults apply until Stage A2 (OwnerContractEnrichmentFunction)
            # resolves the real values.
            ota_related_cost_eur=aggregation.ota_related_cost_eur,
            cleaning_cost_eur=aggregation.cleaning_cost_eur,
            laundry_cost_eur=aggregation.laundry_cost_eur,
            booking_scope_cost_eur=aggregation.booking_scope_cost_eur,
            cost_breakdown=aggregation.cost_breakdown,
            # Phase 19: carried through as-is — resolved by Stage
            # A-correction once occupied_nights/booking_count are available.
            pending_allocation_corrections=aggregation.pending_allocation_corrections,
            property_attribute_factor=property_attribute_factor(
                quality_tier=profile.quality_tier,
                rating=profile.rating,
                has_view=profile.has_view,
                has_parking=profile.has_parking,
            ),
            property_decision_components=tuple(
                property_attribute_components(
                    quality_tier=profile.quality_tier,
                    rating=profile.rating,
                    has_view=profile.has_view,
                    has_parking=profile.has_parking,
                )
            ),
        )

    def process_broadcast_element(self, value, ctx):
        if isinstance(value, ApartmentSegmentRow):
            ctx.get_broadcast_state(SEGMENT_BROADCAST_DESCRIPTOR).put(
                value.apartment_id, value.to_property_pricing_profile()
            )
        elif isinstance(value, PricingStrategyRow):
            strategy_state = ctx.get_broadcast_state(
                PRICING_STRATEGY_BROADCAST_DESCRIPTOR
            )
            current = strategy_state.get(value.apartment_id)
            # Phase 25 (ADR-0018 §4, spec 25 §4 AC-03): version comparison,
            # not arrival order — out-of-order CDC delivery across Kafka
            # partitions must not regress the resolved version.
            if current is None or value.version > current.version:
                strategy_state.put(value.apartment_id, value.to_pricing_strategy())
