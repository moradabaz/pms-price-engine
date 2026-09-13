import json
from dataclasses import replace

from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedBroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor

from flink_jobs.models import (
    CompanyCostOccurrenceRow,
    CostAggregate,
    CostAllocationRuleRow,
    CostDefinitionRow,
)

# Own broadcast state, independent of Stage A0's (each operator's broadcast
# state is private — reusing a MapStateDescriptor instance across operators
# does not share data). Reads its own copy of the same 3 Kafka topics.
COST_DEFINITION_BROADCAST_DESCRIPTOR = MapStateDescriptor(
    "company-stage-cost-definitions", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
COST_ALLOCATION_RULE_BROADCAST_DESCRIPTOR = MapStateDescriptor(
    "company-stage-cost-allocation-rules", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
COMPANY_COST_OCCURRENCE_BROADCAST_DESCRIPTOR = MapStateDescriptor(
    "company-cost-occurrences", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)

_RECURRENCE_MONTHLY_DIVISOR = {
    "per_booking": 1,
    "daily": 1,
    "monthly": 1,
    "quarterly": 3,
    "annual": 12,
    "one_off": 1,
}


class CompanyCostEnrichmentFunction(KeyedBroadcastProcessFunction):
    """Stage A4 (ADR-0011 backlog #13, ADR-0012, spec 19 §5): the corrected
    company-cost fan-out. No applyToKeyedState (unsupported in PyFlink) — a
    company-scoped CostDefinition's occurrences broadcast to every
    apartment, and each apartment applies its own weighted share when its
    own next event reaches this stage (the same "broadcast read from the
    keyed side" shape Stage A2's owner-contract join already uses). Chained
    after Stage A-correction, before Stage A2."""

    def process_element(self, value: CostAggregate, ctx):
        definitions = ctx.get_broadcast_state(COST_DEFINITION_BROADCAST_DESCRIPTOR)
        allocations = ctx.get_broadcast_state(
            COST_ALLOCATION_RULE_BROADCAST_DESCRIPTOR
        )
        occurrences = ctx.get_broadcast_state(
            COMPANY_COST_OCCURRENCE_BROADCAST_DESCRIPTOR
        )

        fixed_addition = 0.0
        variable_addition = 0.0
        for occurrence in occurrences.values():
            definition = definitions.get(occurrence.cost_definition_id)
            allocation = allocations.get(occurrence.cost_definition_id)
            if definition is None or allocation is None:
                # Not delivered yet — skip this occurrence only, same
                # "skip, don't buffer" rule as every other join here.
                continue
            if definition.scope != "company" or allocation.weight_config is None:
                # Defensive: ADR-0012's design never produces this, but a
                # malformed row should be ignored, not crash the pipeline.
                continue

            weight_map = json.loads(allocation.weight_config)
            weight = weight_map.get(value.apartment_id, 0.0)
            if weight <= 0.0:
                continue

            divisor = _RECURRENCE_MONTHLY_DIVISOR.get(definition.recurrence, 1)
            monthly_equivalent = occurrence.amount_gross / divisor
            apartment_share = monthly_equivalent * weight
            per_night_eur = (
                round(apartment_share / value.available_days, 2)
                if value.available_days > 0
                else 0.0
            )

            if definition.behavior == "fixed":
                fixed_addition += per_night_eur
            else:
                variable_addition += per_night_eur

        if fixed_addition == 0.0 and variable_addition == 0.0:
            yield value
            return

        yield replace(
            value,
            fixed_cost_eur=round(value.fixed_cost_eur + fixed_addition, 2),
            variable_cost_eur=round(value.variable_cost_eur + variable_addition, 2),
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
        elif isinstance(value, CompanyCostOccurrenceRow):
            ctx.get_broadcast_state(
                COMPANY_COST_OCCURRENCE_BROADCAST_DESCRIPTOR
            ).put(value.company_cost_occurrence_id, value)
