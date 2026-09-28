import time
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

# window (5min) + allowed lateness (spec 26 §4.4).
DEDUP_TTL_MINUTES = 6


class BookingCreatedDedupFunction(KeyedProcessFunction):
    """Deduplicates booking events by booking_id, first-seen wins (spec 26
    §4.4). Must be keyed by booking_id — not apartment_id — so the
    ValueState is scoped per booking, and redelivery of the same booking
    never passes through twice."""

    def open(self, runtime_context):
        ttl_config = (
            StateTtlConfig.new_builder(Time.minutes(DEDUP_TTL_MINUTES))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        descriptor = ValueStateDescriptor("seen_booking_created", Types.BOOLEAN())
        descriptor.enable_time_to_live(ttl_config)
        self.seen_state = runtime_context.get_state(descriptor)

    def process_element(self, value: Booking, ctx: "KeyedProcessFunction.Context"):
        if not self.seen_state.value():
            return
        self.seen_state.update(True)
        yield value


def booking_id_key(booking: Booking) -> str:
    return str(booking.booking_id)


def apartment_key(booking: Booking) -> str:
    return booking.apartment_id


class BookingCreatedWindowFunction(ProcessWindowFunction):
    """Counts unique booking_ids per apartment/window (spec 26 §4.2). The
    dedup step upstream already guarantees each booking_id appears at most
    once, so this only needs len(elements), never a set()."""

    def process(self, key: str, context, elements):
        bookings = list(elements)
        window = context.window()
        yield {
            "apartment_id": key,
            "window_start": window.start,
            "window_end": window.end,
            "booking_count": len(bookings),
        }


class BookingCreatedSinkFunction(MapFunction):
    """Writes each bookings_created_5min row via put_item, keyed by
    apartment_id/window_start (spec 26 §5.2)."""

    def __init__(
        self, table_name: str, endpoint_url: str | None, region_name: str = "eu-west-1"
    ):
        self.table_name = table_name
        self.endpoint_url = endpoint_url
        self.region_name = region_name

    def open(self, runtime_context):
        self.client = boto3.client(
            "dynamodb", region_name=self.region_name, endpoint_url=self.endpoint_url
        )

    def map(self, value: dict[str, Any]) -> dict[str, Any]:
        item = {
            "apartment_id": {"S": value["apartment_id"]},
            "window_start": {"S": str(value["window_start"])},
            "window_end": {"S": str(value["window_end"])},
            "booking_count": {"N": str(value["booking_count"])},
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
