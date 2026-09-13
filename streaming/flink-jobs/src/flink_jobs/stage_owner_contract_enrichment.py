from dataclasses import replace

from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedBroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor

from flink_jobs.models import CostAggregate, CostDefinitionRow, OwnerContractRow

OWNER_CONTRACT_BROADCAST_DESCRIPTOR = MapStateDescriptor(
    "owner-contract-assignments", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
# Phase 20 (ADR-0013 §4): a SECOND descriptor for this stage's own broadcast
# connection to the cost_definitions stream — job.py reuses the same
# cost_definition_stream DataStream Stage A0 parses, unioned with
# owner_contracts and broadcast under this stage's own descriptor (same
# "one source stream, several independently-connected broadcasts, each with
# its own descriptor" pattern Stage A0/A4 already established for
# cost_definitions/cost_allocation_rules).
OWNER_COMMISSION_COST_DEFINITION_BROADCAST_DESCRIPTOR = MapStateDescriptor(
    "owner-commission-cost-definitions", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)


class OwnerContractEnrichmentFunction(KeyedBroadcastProcessFunction):
    """Stage A2 (ADR-0011 backlog #5, spec 11 §2/§C; rewired by ADR-0013 §4):
    resolves each CostAggregate's commission_pct/commission_base from the
    apartment's own owner-commission CostDefinition — reached via
    owner_contracts.cost_definition_id (this stage's own broadcast state)
    into cost_definitions' rate/revenue_base (a second broadcast state on
    the same connected stream). Chained after Stage A rather than a second
    broadcast input on CostEnrichmentFunction, since PyFlink's
    KeyedStream.connect() accepts exactly one broadcast stream per
    process() call. Emits CostAggregate with commission_base/commission_pct
    resolved."""

    def process_element(self, value: CostAggregate, ctx):
        owner_contracts = ctx.get_broadcast_state(OWNER_CONTRACT_BROADCAST_DESCRIPTOR)
        cost_definitions = ctx.get_broadcast_state(
            OWNER_COMMISSION_COST_DEFINITION_BROADCAST_DESCRIPTOR
        )

        assignment = owner_contracts.get(value.apartment_id)
        if assignment is None:
            # Owner contract not delivered yet — skip, do not buffer, same
            # rule Stage A already applies for missing segment config.
            return
        definition = cost_definitions.get(assignment.cost_definition_id)
        if definition is None:
            # This apartment's own commission CostDefinition hasn't arrived
            # yet either — same skip rule.
            return

        yield replace(
            value,
            commission_base=definition.revenue_base or "total_revenue",
            commission_pct=definition.rate or 0.0,
        )

    def process_broadcast_element(self, value, ctx):
        if isinstance(value, OwnerContractRow):
            ctx.get_broadcast_state(OWNER_CONTRACT_BROADCAST_DESCRIPTOR).put(
                value.apartment_id, value.to_assignment()
            )
        elif isinstance(value, CostDefinitionRow):
            ctx.get_broadcast_state(
                OWNER_COMMISSION_COST_DEFINITION_BROADCAST_DESCRIPTOR
            ).put(value.cost_definition_id, value)
