from dataclasses import replace
from datetime import timedelta

from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedCoProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

from flink_jobs.models import BookingRow, CostAggregate
from flink_jobs.occupancy import booking_count, occupied_nights

BOOKINGS_STATE_DESCRIPTOR = MapStateDescriptor(
    "bookings-by-booking-id", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
LATEST_COST_AGGREGATE_DESCRIPTOR = ValueStateDescriptor(
    "latest-cost-aggregate", Types.PICKLED_BYTE_ARRAY()
)

# Generous buffer past the current billing period — bounds state growth
# without risking evicting a booking the current occupancy calculation
# could still need. Not the same "2 distinct period ends" rule payment
# lines use (spec 18 §4): bookings don't cluster into discrete,
# non-overlapping periods the way billing_period_end does.
_RETENTION_BUFFER_DAYS = 90


class BookingEnrichmentFunction(KeyedCoProcessFunction):
    """Stage A-bookings (ADR-0011 backlog #13 prerequisite, spec 18 §4):
    chained right after Stage A (CostEnrichmentFunction), before Stage A2
    (OwnerContractEnrichmentFunction). A regular two-keyed-stream join, both
    sides keyed by apartment_id — the same shape as Stage B's cost/market
    join, not a broadcast. Emits CostAggregate with occupied_nights and
    booking_count resolved (Phase 19 adds the latter, additive)."""

    def open(self, runtime_context):
        self.bookings = runtime_context.get_map_state(BOOKINGS_STATE_DESCRIPTOR)
        self.latest_cost_aggregate = runtime_context.get_state(
            LATEST_COST_AGGREGATE_DESCRIPTOR
        )

    def _recompute(self, cost_aggregate: CostAggregate) -> CostAggregate:
        bookings = list(self.bookings.values())
        nights = occupied_nights(
            bookings,
            cost_aggregate.billing_period_start,
            cost_aggregate.billing_period_end,
        )
        # Phase 19 (ADR-0011 backlog #13): booking_count alongside
        # occupied_nights, additive to this already-verified stage.
        count = booking_count(
            bookings,
            cost_aggregate.billing_period_start,
            cost_aggregate.billing_period_end,
        )
        return replace(cost_aggregate, occupied_nights=nights, booking_count=count)

    def process_element1(self, value: CostAggregate, ctx):
        """Cost side (Stage A's output): cache it, recompute occupied_nights
        from whatever bookings are already known, re-emit."""
        self.latest_cost_aggregate.update(value)
        yield self._recompute(value)

    def process_element2(self, value: BookingRow, ctx):
        """Booking side: upsert, prune, then — if a cost aggregate has
        already been seen for this apartment — recompute and re-emit it. A
        booking change can shift occupancy for a period Stage A already
        emitted, without needing a new cost event to notice. Skip (no
        emission) if no cost aggregate has arrived yet — nothing to enrich,
        same "skip, don't buffer" rule every other join stage here follows
        for a not-yet-known counterpart."""
        self.bookings.put(value.booking_id, value)

        cached = self.latest_cost_aggregate.value()
        if cached is not None:
            cutoff = cached.billing_period_start - timedelta(
                days=_RETENTION_BUFFER_DAYS
            )
            for booking_id, row in dict(self.bookings.items()).items():
                if row.check_out < cutoff:
                    self.bookings.remove(booking_id)

        if cached is None:
            return

        yield self._recompute(cached)
