from dataclasses import replace

from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedBroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor

from flink_jobs.models import CostAggregate, OwnerContractRow

OWNER_CONTRACT_BROADCAST_DESCRIPTOR = MapStateDescriptor(
    "owner-contract-assignments", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)


class OwnerContractEnrichmentFunction(KeyedBroadcastProcessFunction):
    """Stage A2 (ADR-0011 backlog #5, spec 11 §2/§C): resolves each
    CostAggregate's commission_base/commission_pct from the owner_contracts
    broadcast stream — chained after Stage A rather than a second broadcast
    input on CostEnrichmentFunction, since PyFlink's KeyedStream.connect()
    accepts exactly one broadcast stream per process() call. Emits
    CostAggregate with commission_base/commission_pct resolved."""

    def process_element(self, value: CostAggregate, ctx):
        broadcast_state = ctx.get_broadcast_state(OWNER_CONTRACT_BROADCAST_DESCRIPTOR)
        assignment = broadcast_state.get(value.apartment_id)
        if assignment is None:
            # Owner contract not delivered yet — skip, do not buffer, same
            # rule Stage A already applies for missing segment config.
            return

        yield replace(
            value,
            commission_base=assignment.commission_base,
            commission_pct=assignment.commission_pct,
        )

    def process_broadcast_element(self, value: OwnerContractRow, ctx):
        ctx.get_broadcast_state(OWNER_CONTRACT_BROADCAST_DESCRIPTOR).put(
            value.apartment_id, value.to_assignment()
        )
