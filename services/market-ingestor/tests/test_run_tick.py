import random
from typing import Any

from market_ingestor.main import run_tick
from market_ingestor.segments import SEGMENTS
from market_ingestor.settings import MarketIngestorSettings


class FakeKinesisClient:
    def __init__(self) -> None:
        self.put_records_calls: list[dict[str, Any]] = []

    def put_records(self, **kwargs: Any) -> dict:
        self.put_records_calls.append(kwargs)
        return {"Records": [{} for _ in kwargs["Records"]]}


def test_run_tick_publishes_blended_plus_three_channel_events_per_segment():
    # Phase 16 (ADR-0011 backlog #2): 4 records per segment now (1 blended,
    # unchanged, + 3 channel-specific) instead of 1 — market-ingestor's own
    # emission volume is what multiplies by channel count, not Stage B's
    # (spec 16 §1).
    client = FakeKinesisClient()
    settings = MarketIngestorSettings(kinesis_stream_name="market-price-events")
    rng = random.Random(7)

    run_tick(client, settings, rng, tick_count=0)

    assert len(client.put_records_calls) == 1
    records = client.put_records_calls[0]["Records"]
    assert len(records) == len(SEGMENTS) * 4
