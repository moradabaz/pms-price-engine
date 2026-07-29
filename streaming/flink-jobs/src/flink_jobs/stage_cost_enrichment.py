from datetime import UTC, datetime

from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedBroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from shared_schemas.payment_line import PaymentLine

from flink_jobs.cost_aggregation import aggregate_cost, retained_billing_period_ends
from flink_jobs.models import ApartmentSegmentRow, CostAggregate

SEGMENT_BROADCAST_DESCRIPTOR = MapStateDescriptor(
    "apartment-segment-assignments", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
COST_LINES_STATE_DESCRIPTOR = MapStateDescriptor(
    "payment-lines-by-event-id", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)


class CostEnrichmentFunction(KeyedBroadcastProcessFunction):
    """Stage A: aggregates cost per apartment and enriches it with segment
    and margin config from broadcast state. Emits CostAggregate."""

    def open(self, runtime_context):
        self.cost_lines_state = runtime_context.get_map_state(
            COST_LINES_STATE_DESCRIPTOR
        )

    def process_element(self, value: PaymentLine, ctx):
        self.cost_lines_state.put(str(value.event_id), value)

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

        broadcast_state = ctx.get_broadcast_state(SEGMENT_BROADCAST_DESCRIPTOR)
        assignment = broadcast_state.get(value.apartment_id)
        if assignment is None:
            # Segment not delivered yet — skip, do not buffer (spec §6).
            return

        yield CostAggregate(
            apartment_id=value.apartment_id,
            apartment_reference=value.apartment_reference,
            city=assignment.city,
            neighborhood=assignment.neighborhood,
            property_type=assignment.property_type,
            bedrooms=assignment.bedrooms,
            daily_cost_eur=aggregation.daily_cost_eur,
            total_monthly_cost_eur=aggregation.total_monthly_cost_eur,
            available_days=aggregation.available_days,
            cost_lines_count=aggregation.cost_lines_count,
            billing_period_start=aggregation.billing_period_start,
            billing_period_end=aggregation.billing_period_end,
            target_margin=assignment.target_margin,
            competitiveness_discount=assignment.competitiveness_discount,
            updated_at=datetime.now(UTC),
        )

    def process_broadcast_element(self, value: ApartmentSegmentRow, ctx):
        ctx.get_broadcast_state(SEGMENT_BROADCAST_DESCRIPTOR).put(
            value.apartment_id, value.to_assignment()
        )
