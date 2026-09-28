import time
from datetime import UTC, datetime
from typing import Any

import boto3
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    MapFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from shared_schemas.booking import Booking

# window (5min) + allowed lateness (spec 26 §4.4b) — same TTL policy as
# bookings_created.py's DEDUP_TTL_MINUTES, kept as its own constant here
# because the two dedup functions are deliberately independent (see
# BookingCancelledDedupFunction's docstring below).
DEDUP_TTL_MINUTES = 6


def is_cancelled(booking: Booking) -> bool:
    """Filter predicate: keeps only cancellation events.

    Called from job.py as `booking_stream.filter(is_cancelled)`, right
    after deserialization and before this metric's own key_by/dedup/window
    chain — booking_stream itself is shared with the bookings_created
    pipeline (Bloque 3), so this filter is what scopes this branch to
    cancellations only.
    """
    return booking.status == "cancelled"


def booking_id_key(booking: Booking) -> str:
    """Key selector used for the dedup stage: one state slot per booking.

    Called from job.py as `.key_by(booking_id_key)` immediately before
    `BookingCancelledDedupFunction`, so the ValueState inside that function
    is scoped per booking_id, not per apartment.
    """
    return str(booking.booking_id)


def apartment_key(booking: Booking) -> str:
    """Key selector used for the window stage: groups by apartment.

    Called from job.py as `.key_by(apartment_key)` right after the dedup
    stage, before `.window(...)`, so `BookingCancelledWindowFunction.process`
    receives all deduplicated cancellations for one apartment/window pair.
    """
    return booking.apartment_id


class BookingCancelledDedupFunction(KeyedProcessFunction):
    """Deduplicates cancellation events by booking_id, first-seen wins.

    Deliberately uses its own ValueStateDescriptor ("seen_booking_cancelled"),
    separate from BookingCreatedDedupFunction's ("seen_booking_created") in
    bookings_created.py — spec 26 §4.4b: sharing one descriptor between the
    two would mean a booking's *creation* event marks it "seen", which would
    then incorrectly suppress its later *cancellation* event too. Each
    lifecycle transition needs its own dedup memory.

    `open()` is called once per parallel instance when the operator starts,
    to register the state descriptor with Flink's state backend.
    `process_element()` is then called once per incoming event, and decides
    whether to `yield` it onward or drop it.
    """

    def open(self, runtime_context):
        ttl_config = (
            StateTtlConfig.new_builder(Time.minutes(DEDUP_TTL_MINUTES))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        descriptor = ValueStateDescriptor("seen_booking_cancelled", Types.BOOLEAN())
        descriptor.enable_time_to_live(ttl_config)
        self.seen_state = runtime_context.get_state(descriptor)

    def process_element(self, value: Booking, ctx: "KeyedProcessFunction.Context"):
        # Already seen this booking_id's cancellation before (e.g. Kafka
        # redelivered the same message) -> drop it, do not re-count it.
        if self.seen_state.value():
            return
        # First time seeing this booking_id's cancellation -> remember it,
        # then let it through to the window stage.
        self.seen_state.update(True)
        yield value


class BookingCancelledWindowFunction(ProcessWindowFunction):
    """Counts unique cancelled booking_ids per apartment/window (spec §4.2).

    Called once per (apartment_id, window) pair when the window closes,
    with `elements` already deduplicated by BookingCancelledDedupFunction
    upstream — so counting `len(elements)` is enough, no set() needed.
    Its output dict is passed onward to `.map(booking_cancelled_writer)` in
    job.py.
    """

    def process(self, key: str, context, elements):
        bookings = list(elements)
        window = context.window()
        yield {
            "apartment_id": key,
            "window_start": datetime.fromtimestamp(window.start / 1000, tz=UTC).isoformat(),
            "window_end": datetime.fromtimestamp(window.end / 1000, tz=UTC).isoformat(),
            "cancelled_count": len(bookings),
        }


class BookingCancelledSinkFunction(MapFunction):
    """Writes each bookings_cancelled_5min row via put_item (spec §5.2).

    Called from job.py as `.map(booking_cancelled_writer)`, right before the
    DiscardingSink placeholder — `map()` here is used for its side effect
    (the DynamoDB write), and it returns `value` unchanged so the stream can
    still be attached to a sink afterwards.
    """

    def __init__(
        self, table_name: str, endpoint_url: str | None, region_name: str = "eu-west-1"
    ):
        self.table_name = table_name
        self.endpoint_url = endpoint_url
        self.region_name = region_name

    def open(self, runtime_context):
        # Called once per parallel instance before any map() call — creates
        # one boto3 client per subtask, reused across every record it sees.
        self.client = boto3.client(
            "dynamodb", region_name=self.region_name, endpoint_url=self.endpoint_url
        )

    def map(self, value: dict[str, Any]) -> dict[str, Any]:
        item = {
            "apartment_id": {"S": value["apartment_id"]},
            "window_start": {"S": value["window_start"]},
            "window_end": {"S": value["window_end"]},
            "cancelled_count": {"N": str(value["cancelled_count"])},
        }
        attempt = 0
        while True:
            try:
                self.client.put_item(TableName=self.table_name, Item=item)
                return value
            except Exception:
                attempt += 1
                if attempt > 3:
                    raise
                time.sleep(0.5 * (2**attempt))
