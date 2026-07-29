import random
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from common import configure_logging, get_logger

from market_ingestor.events import build_market_price_event
from market_ingestor.kinesis import KinesisRecord, partition_key, publish_batch
from market_ingestor.segments import SEGMENTS
from market_ingestor.settings import MarketIngestorSettings


def _build_client(settings: MarketIngestorSettings) -> Any:
    return boto3.client(
        "kinesis",
        region_name=settings.aws_region,
        endpoint_url=settings.kinesis_endpoint_url,
    )


def run_tick(
    client: Any, settings: MarketIngestorSettings, rng: random.Random, tick_count: int
) -> None:
    now = datetime.now(UTC)

    # Decision D.1: deterministic cyclic coverage, not a random date per
    # segment. One target_date per tick, shared by every segment — computed
    # fresh from `now` each time (never cached), so the 60-day window slides
    # on its own as real days pass. With the defaults (tick=60s, forecast=60
    # days) a full cycle takes 60×60s=1h: every segment covers all 60 nights,
    # and every night is refreshed at least once per hour in steady state.
    offset_days = 1 + (tick_count % settings.forecast_days)
    target_date = now.date() + timedelta(days=offset_days)

    records: list[KinesisRecord] = []
    for segment in SEGMENTS:
        event = build_market_price_event(segment, target_date, rng, now)
        records.append(
            {
                "Data": event.model_dump_json().encode("utf-8"),
                "PartitionKey": partition_key(segment),
            }
        )

    publish_batch(
        client,
        settings.kinesis_stream_name,
        records,
        settings.publish_max_retries,
        settings.publish_backoff_base_seconds,
    )
    get_logger(__name__).info(
        "tick_complete", segments_published=len(records), target_date=target_date.isoformat()
    )


def run_forever(client: Any, settings: MarketIngestorSettings) -> None:
    rng = random.Random()
    tick_count = 0
    while True:
        run_tick(client, settings, rng, tick_count)
        tick_count += 1
        time.sleep(settings.tick_interval_seconds)


def main() -> None:
    settings = MarketIngestorSettings()
    configure_logging(settings.log_level)
    logger = get_logger(__name__)

    client = _build_client(settings)
    logger.info(
        "market_ingestor_starting",
        segments=len(SEGMENTS),
        stream_name=settings.kinesis_stream_name,
    )
    run_forever(client, settings)


if __name__ == "__main__":
    main()
