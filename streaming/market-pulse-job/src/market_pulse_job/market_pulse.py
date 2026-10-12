import time
from datetime import UTC, datetime
from typing import Any

import boto3
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import (
    AggregateFunction,
    MapFunction,
    ProcessWindowFunction,
)
from shared_schemas.market_price import MarketPrice

# Accumulator layout (spec 26 §4.1): (weighted_sum, weight_total, min_price,
# max_price, snapshot_count). weighted_sum/weight_total let the final
# division happen once, in get_result() — never recompute a mean from a
# mean (docs/tech-concepts/windowing-design-checklist.md §3).
ACC_TYPE = Types.TUPLE(
    [Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.LONG()]
)


def is_blended_snapshot(event: MarketPrice) -> bool:
    """True for the top-level, per-segment snapshot — excludes the
    per-channel snapshots Phase 16 also publishes (spec 26 §4.1, AC-02)."""
    return event.market_context.platform is None


def segment_key(event: MarketPrice) -> str:
    """Builds the full-segment key: market_area + property type + bedrooms
    (spec 26 §4.1) — not market_area alone, which would mix incomparable
    property types under one average."""
    neighborhood = event.market_area.neighborhood or ""
    market_area = f"{event.market_area.city}/{neighborhood}".rstrip("/")
    profile = event.property_profile
    return f"{market_area}|{profile.type}|{profile.bedrooms}"


class MarketPulseAggregateFunction(AggregateFunction):
    """Weighted-average AggregateFunction for the market pulse metric (spec
    26 §4.1). Weighted by market_context.sample_size, not a plain average
    of avg_nightly_rate — a low-sample_size snapshot must not outweigh a
    high-confidence one."""

    def create_accumulator(self) -> tuple[float, float, float, float, int]:
        return (0.0, 0.0, float("inf"), -float("inf"), 0)

    def add(
        self, value: MarketPrice, accumulator: tuple[float, float, float, float, int]
    ) -> tuple[float, float, float, float, int]:
        weighted_sum, weight_total, min_price, max_price, count = accumulator
        rate = value.pricing.avg_nightly_rate
        weight = value.market_context.sample_size
        return (
            weighted_sum + rate * weight,
            weight_total + weight,
            min(min_price, rate),
            max(max_price, rate),
            count + 1,
        )

    def get_result(
        self, accumulator: tuple[float, float, float, float, int]
    ) -> dict[str, Any]:
        weighted_sum, weight_total, min_price, max_price, count = accumulator
        return {
            "avg_price_eur": weighted_sum / weight_total,
            "min_price_eur": min_price,
            "max_price_eur": max_price,
            "sample_count": count,
        }

    def merge(
        self,
        acc_a: tuple[float, float, float, float, int],
        acc_b: tuple[float, float, float, float, int],
    ) -> tuple[float, float, float, float, int]:
        return (
            acc_a[0] + acc_b[0],
            acc_a[1] + acc_b[1],
            min(acc_a[2], acc_b[2]),
            max(acc_a[3], acc_b[3]),
            acc_a[4] + acc_b[4],
        )


class MarketPulseWindowFunction(ProcessWindowFunction):
    """Attaches the segment key and window boundaries to the aggregated
    result — the AggregateFunction alone only knows the accumulator."""

    def process(self, key: str, context, elements):
        (aggregated,) = elements
        window = context.window()
        yield {
            "segment_key": key,
            "window_start": datetime.fromtimestamp(
                window.start / 1000, tz=UTC
            ).isoformat(),
            "window_end": datetime.fromtimestamp(window.end / 1000, tz=UTC).isoformat(),
            **aggregated,
        }


class MarketPulseSinkFunction(MapFunction):
    """Writes each market_pulse_5min row via put_item, keyed by
    segment_key/window_start (spec 26 §5.2)."""

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
            "segment_key": {"S": value["segment_key"]},
            "window_start": {"S": value["window_start"]},
            "window_end": {"S": value["window_end"]},
            "avg_price_eur": {"N": str(value["avg_price_eur"])},
            "min_price_eur": {"N": str(value["min_price_eur"])},
            "max_price_eur": {"N": str(value["max_price_eur"])},
            "sample_count": {"N": str(value["sample_count"])},
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
