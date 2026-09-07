from datetime import UTC, datetime

from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedBroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from shared_schemas.price_decision import (
    DecisionComponent,
    ManualOverrideDetails,
    PriceDecision,
)

from flink_jobs.models import ManualOverrideAssignment, ManualOverrideRow

MANUAL_OVERRIDE_BROADCAST_DESCRIPTOR = MapStateDescriptor(
    "manual-override-assignments", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)


def apply_manual_override(
    decision: PriceDecision, assignment: ManualOverrideAssignment
) -> PriceDecision:
    """Overwrites decision's published price with an authorized override
    (Phase 14, ADR-0011 backlog #9, spec 14 §4). Pure function, no Flink
    dependency — the caller (process_element) is responsible for checking
    assignment.valid_until before calling this. Deliberately unchanged:
    calculation.rule_applied (kept as the algorithm's own, informational
    classification — the same "orthogonal axis" reasoning Phase 12 used for
    floor_policy), calculation.los_floor_matrix (an override applies to one
    specific (apartment_id, target_date), never per-LOS-candidate), and
    output.below_market_by (kept as the algorithm's own market comparison,
    not recomputed against the override). Returns the enriched PriceDecision."""
    total_cost_eur = (
        decision.cost_inputs.fixed_cost_eur
        + decision.cost_inputs.variable_cost_eur
        + decision.cost_inputs.one_time_cost_eur
    )
    new_effective_margin = (
        (assignment.override_price_eur / total_cost_eur) - 1 if total_cost_eur else 0.0
    )
    # Clamped at zero — an override at or above the algorithmic floor is
    # still audited but registers no loss (spec 14 §4).
    floor_gap = decision.calculation.minimum_price_eur - assignment.override_price_eur
    expected_loss_eur = round(max(0.0, floor_gap), 2)
    override_component = DecisionComponent(
        code="manual_override_applied",
        label=(
            f"Manual override by {assignment.authorized_by}: {assignment.reason} "
            f"(published {assignment.override_price_eur} EUR instead of the "
            f"{decision.output.suggested_price_eur} EUR algorithmic suggestion)"
        ),
        impact=round(
            assignment.override_price_eur - decision.output.suggested_price_eur, 2
        ),
    )
    return decision.model_copy(
        update={
            "calculation": decision.calculation.model_copy(
                update={
                    "manual_override": ManualOverrideDetails(
                        override_price_eur=assignment.override_price_eur,
                        reason=assignment.reason,
                        authorized_by=assignment.authorized_by,
                        valid_until=assignment.valid_until,
                        expected_loss_eur=expected_loss_eur,
                    ),
                    "decision_components": [
                        *decision.calculation.decision_components,
                        override_component,
                    ],
                }
            ),
            "output": decision.output.model_copy(
                update={
                    "suggested_price_eur": assignment.override_price_eur,
                    "effective_margin": round(new_effective_margin, 4),
                }
            ),
        }
    )


class ManualOverrideEnrichmentFunction(KeyedBroadcastProcessFunction):
    """Stage C (ADR-0011 backlog #9, spec 14 §2/§4): the first stage in this
    project chained AFTER the pricing decision itself, not before it. Looks
    up an unexpired manual override for this decision's (apartment_id,
    target_date) and, if one exists, republishes it in place of the
    algorithmic suggestion — audited, never silent. Emits PriceDecision,
    overridden or unchanged."""

    def process_element(self, value: PriceDecision, ctx):
        key = f"{value.apartment_id}:{value.target_date.isoformat()}"
        broadcast_state = ctx.get_broadcast_state(MANUAL_OVERRIDE_BROADCAST_DESCRIPTOR)
        assignment = broadcast_state.get(key)
        if assignment is None or datetime.now(UTC) > assignment.valid_until:
            # No active override — pass through unchanged. Expired entries
            # are left in broadcast state (not evicted); see spec 14 §7.
            yield value
            return
        yield apply_manual_override(value, assignment)

    def process_broadcast_element(self, value: ManualOverrideRow, ctx):
        ctx.get_broadcast_state(MANUAL_OVERRIDE_BROADCAST_DESCRIPTOR).put(
            value.broadcast_key(), value.to_assignment()
        )
