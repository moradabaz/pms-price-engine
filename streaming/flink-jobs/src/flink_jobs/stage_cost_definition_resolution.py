from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedBroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from shared_schemas.payment_line import PaymentLine

from flink_jobs.cost_aggregation import EnrichedPaymentLine
from flink_jobs.models import CostAllocationRuleRow, CostDefinitionRow

# Two broadcast states fed by ONE unioned broadcast stream (job.py unions
# cost_definitions and cost_allocation_rules before broadcasting) — PyFlink's
# DataStream.broadcast() accepts multiple MapStateDescriptors for exactly
# this shape, so this doesn't need a second chained stage the way Stage
# A2/A4 do (those broadcast genuinely different-shaped, independently-timed
# streams onto an already-keyed CostAggregate).
COST_DEFINITION_BROADCAST_DESCRIPTOR = MapStateDescriptor(
    "cost-definitions", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
COST_ALLOCATION_RULE_BROADCAST_DESCRIPTOR = MapStateDescriptor(
    "cost-allocation-rules", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)


class CostDefinitionResolutionFunction(KeyedBroadcastProcessFunction):
    """Stage A0 (ADR-0011 backlog #13, ADR-0012, spec 19 §4): resolves each
    PaymentLine's cost_definition_id against the cost_definitions/
    cost_allocation_rules broadcast state, chained before Stage A. Emits
    EnrichedPaymentLine — Stage A never reads PaymentLine.cost_definition_id
    itself."""

    def process_element(self, value: PaymentLine, ctx):
        definitions = ctx.get_broadcast_state(COST_DEFINITION_BROADCAST_DESCRIPTOR)
        allocations = ctx.get_broadcast_state(COST_ALLOCATION_RULE_BROADCAST_DESCRIPTOR)

        definition = definitions.get(str(value.cost_definition_id))
        allocation = allocations.get(str(value.cost_definition_id))
        if definition is None or allocation is None:
            # Not delivered yet — skip, do not buffer (same rule every
            # broadcast join here already follows for missing config).
            return

        yield EnrichedPaymentLine(
            event_id=str(value.event_id),
            apartment_id=value.apartment_id,
            apartment_reference=value.apartment_reference,
            billing_period_start=value.billing_period_start,
            billing_period_end=value.billing_period_end,
            amount_gross=value.amount_gross,
            concept=definition.concept,
            scope=definition.scope,
            behavior=definition.behavior,
            trigger=definition.trigger,
            calculation_base=definition.calculation_base,
            recurrence=definition.recurrence,
            revenue_base=definition.revenue_base,
            allocation_method=allocation.method,
            weight_config=allocation.weight_config,
        )

    def process_broadcast_element(self, value, ctx):
        if isinstance(value, CostDefinitionRow):
            ctx.get_broadcast_state(COST_DEFINITION_BROADCAST_DESCRIPTOR).put(
                value.cost_definition_id, value
            )
        elif isinstance(value, CostAllocationRuleRow):
            ctx.get_broadcast_state(COST_ALLOCATION_RULE_BROADCAST_DESCRIPTOR).put(
                value.cost_definition_id, value
            )
