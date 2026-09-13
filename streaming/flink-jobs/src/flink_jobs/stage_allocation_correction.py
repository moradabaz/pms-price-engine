from dataclasses import replace

from pyflink.datastream.functions import MapFunction

from flink_jobs.models import CostAggregate

# Falls back to calendar_day's own divisor when the true denominator is
# unavailable (e.g. a concept allocated by occupied_night with zero
# occupied nights this period) — never divides by zero, and a 0-occupancy
# period is exactly when "spread over the whole period" is the most
# reasonable fallback anyway.


class AllocationCorrectionFunction(MapFunction):
    """Stage A-correction (ADR-0011 backlog #13, spec 19 §4): chained after
    Stage A-bookings (Phase 18), once occupied_nights/booking_count are
    available on CostAggregate. Resolves every pending_allocation_correction
    (concepts whose allocation_method needed data Stage A didn't have yet)
    into a real per-night figure, folds it into fixed_cost_eur/
    variable_cost_eur, and clears the tuple so it is never applied twice."""

    def map(self, value: CostAggregate) -> CostAggregate:
        if not value.pending_allocation_corrections:
            return value

        fixed_addition = 0.0
        variable_addition = 0.0
        for correction in value.pending_allocation_corrections:
            if correction.allocation_method == "occupied_night":
                denominator = value.occupied_nights
            elif correction.allocation_method == "booking":
                denominator = value.booking_count
            else:
                denominator = 0

            if denominator <= 0:
                denominator = value.available_days

            per_night_eur = (
                round(correction.monthly_equivalent_eur / denominator, 2)
                if denominator > 0
                else 0.0
            )

            if correction.behavior == "fixed":
                fixed_addition += per_night_eur
            else:
                variable_addition += per_night_eur

        return replace(
            value,
            fixed_cost_eur=round(value.fixed_cost_eur + fixed_addition, 2),
            variable_cost_eur=round(value.variable_cost_eur + variable_addition, 2),
            pending_allocation_corrections=(),
        )
