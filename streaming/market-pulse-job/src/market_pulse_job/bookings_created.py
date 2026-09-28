import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

import boto3
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import (
    AsyncFunction,
    KeyedProcessFunction,
    MapFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from shared_schemas.booking import Booking

logger = logging.getLogger(__name__)

# window (5min) + allowed lateness (spec 26 §4.4).
DEDUP_TTL_MINUTES = 6


class BookingCreatedDedupFunction(KeyedProcessFunction):
    """Deduplicates booking events by booking_id, first-seen wins (spec 26
    §4.4). Must be keyed by booking_id — not apartment_id — so the
    ValueState is scoped per booking, and redelivery of the same booking
    never passes through twice.

    `open()` is called once per parallel instance when the operator starts.
    `process_element()` is called once per incoming event; it yields the
    event onward the first time a booking_id is seen, and drops it on every
    later occurrence."""

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
        if self.seen_state.value():
            return
        self.seen_state.update(True)
        yield value


def booking_id_key(booking: Booking) -> str:
    """Key selector feeding the dedup stage (one state slot per booking).
    Called from job.py as `.key_by(booking_id_key)`, right before
    `BookingCreatedDedupFunction`."""
    return str(booking.booking_id)


def enriched_apartment_key(enriched: dict[str, Any]) -> str:
    """Key selector feeding the window stage — called AFTER async
    enrichment (spec §2: dedupCreated --> asyncEnrich --> createdWin), so it
    reads `apartment_id` off the enrichment output dict (booking + resolved
    profit_eur, possibly None), not off a bare Booking. Called from job.py
    as `.key_by(enriched_apartment_key)`, right before `.window(...)`."""
    return enriched["booking"].apartment_id


class BookingProfitEnrichmentFunction(AsyncFunction):
    """Async I/O lookup of each booking's profit basis against the existing
    `price_decision` table (spec §4.5) — the piece this whole block exists
    to teach. `async_invoke()` is called once per deduplicated booking
    coming out of `BookingCreatedDedupFunction`, and is a coroutine: it
    offloads boto3's synchronous `get_item` onto `self.executor` via
    `loop.run_in_executor`, so the Python worker's event loop stays free to
    juggle other bookings' lookups concurrently (up to `capacity`, set on
    `AsyncDataStream.unordered_wait` in job.py) instead of blocking one at a
    time.

    Never drops a booking, whatever happens: `async_invoke()` always
    returns exactly one dict with the original booking plus either a
    resolved `profit_eur` or `None`. `timeout()` is called by the Flink
    runtime instead of `async_invoke()`'s result when the lookup takes
    longer than the timeout passed to `AsyncDataStream.unordered_wait` — it
    returns the same "unresolved" shape rather than failing the job.
    `open()`/`close()` set up and tear down the boto3 client and thread
    pool once per parallel instance, not once per booking."""

    def __init__(
        self,
        table_name: str,
        endpoint_url: str | None,
        region_name: str = "eu-west-1",
        max_workers: int = 8,
    ):
        self.table_name = table_name
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.max_workers = max_workers

    def open(self, runtime_context):
        self.client = boto3.client(
            "dynamodb", region_name=self.region_name, endpoint_url=self.endpoint_url
        )
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)

    def close(self):
        self.executor.shutdown(wait=True)

    async def async_invoke(self, value: Booking) -> list[dict[str, Any]]:
        loop = asyncio.get_event_loop()
        try:
            profit_eur = await loop.run_in_executor(self.executor, self._resolve_profit, value)
        except Exception:
            logger.warning(
                "price_decision_lookup_failed booking_id=%s apartment_id=%s",
                value.booking_id,
                value.apartment_id,
            )
            profit_eur = None
        return [{"booking": value, "profit_eur": profit_eur}]

    def timeout(self, value: Booking) -> list[dict[str, Any]]:
        logger.warning(
            "price_decision_lookup_timed_out booking_id=%s apartment_id=%s",
            value.booking_id,
            value.apartment_id,
        )
        return [{"booking": value, "profit_eur": None}]

    def _resolve_profit(self, booking: Booking) -> float | None:
        """Runs on `self.executor`'s thread pool, called from
        `async_invoke()` via `run_in_executor` — never called directly on
        the event-loop thread. Fetches the `price_decision` row for this
        booking's `(apartment_id, target_date=check_in)` and evaluates the
        break-even formula (spec §4.5) against this booking's actual
        `revenue_eur`. Returns `None` — never `0.0` — when no
        `price_decision` exists yet for that key, so a missing cost basis
        is never conflated with a genuinely zero-profit booking."""
        response = self.client.get_item(
            TableName=self.table_name,
            Key={
                "apartment_id": {"S": booking.apartment_id},
                "target_date": {"S": booking.check_in.isoformat()},
            },
        )
        item = response.get("Item")
        if item is None:
            logger.warning(
                "price_decision_not_found booking_id=%s apartment_id=%s target_date=%s",
                booking.booking_id,
                booking.apartment_id,
                booking.check_in.isoformat(),
            )
            return None
        cost_inputs = item["cost_inputs"]["M"]
        fixed_and_allocated_costs_eur = float(cost_inputs["fixed_and_allocated_costs_eur"]["N"])
        per_booking_cost_eur = float(cost_inputs["per_booking_cost_eur"]["N"])
        p = float(cost_inputs["p"]["N"])
        nights = (booking.check_out - booking.check_in).days
        total_cost_eur = fixed_and_allocated_costs_eur * nights + per_booking_cost_eur
        return booking.revenue_eur * (1 - p) + total_cost_eur


class BookingCreatedWindowFunction(ProcessWindowFunction):
    """Counts bookings and averages resolved profit per apartment/window
    (spec §4.2, §4.4, §4.5). Rewritten in Bloque 5 (was Bloque 3's plain
    counter): `process()` is still called once per (apartment_id, window)
    pair when the window closes, but `elements` now holds the
    asyncEnrich output (booking + optional profit_eur) instead of a bare
    Booking, per the architecture diagram in spec §2.

    `booking_count` stays unconditional — every deduplicated booking,
    resolved or not. `avg_profit_eur`/`profit_sample_count` only cover the
    subset that got a resolved cost basis; `unresolved_cost_count` is
    everything else in the same window."""

    def process(self, key: str, context, elements):
        enriched = list(elements)
        booking_count = len(enriched)
        profits = [e["profit_eur"] for e in enriched if e["profit_eur"] is not None]
        profit_sample_count = len(profits)
        unresolved_cost_count = booking_count - profit_sample_count
        avg_profit_eur = sum(profits) / profit_sample_count if profit_sample_count else None
        window = context.window()
        yield {
            "apartment_id": key,
            "window_start": datetime.fromtimestamp(window.start / 1000, tz=UTC).isoformat(),
            "window_end": datetime.fromtimestamp(window.end / 1000, tz=UTC).isoformat(),
            "booking_count": booking_count,
            "avg_profit_eur": avg_profit_eur,
            "profit_sample_count": profit_sample_count,
            "unresolved_cost_count": unresolved_cost_count,
        }


class BookingCreatedSinkFunction(MapFunction):
    """Writes each bookings_created_5min row via put_item, keyed by
    apartment_id/window_start (spec §5.2) — now including the profit
    metrics computed in Bloque 5. `map()` is called once per closed window
    (once per dict yielded by `BookingCreatedWindowFunction.process`), and
    returns the value unchanged so a sink can still be attached downstream.

    `avg_profit_eur` is written as DynamoDB's `NULL` type, not `0`, when
    every booking in the window lacked a cost basis — spec §4.5 is explicit
    that a missing cost basis and a genuinely zero-profit booking must
    never be conflated."""

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
        avg_profit_eur = value["avg_profit_eur"]
        item = {
            "apartment_id": {"S": value["apartment_id"]},
            "window_start": {"S": value["window_start"]},
            "window_end": {"S": value["window_end"]},
            "booking_count": {"N": str(value["booking_count"])},
            "avg_profit_eur": (
                {"NULL": True} if avg_profit_eur is None else {"N": str(avg_profit_eur)}
            ),
            "profit_sample_count": {"N": str(value["profit_sample_count"])},
            "unresolved_cost_count": {"N": str(value["unresolved_cost_count"])},
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
