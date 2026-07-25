import time
from typing import Any, TypedDict

from common import get_logger

from market_ingestor.segments import Segment

logger = get_logger(__name__)


class KinesisRecord(TypedDict):
    Data: bytes
    PartitionKey: str


def partition_key(segment: Segment) -> str:
    """Plain string per ADR-0005 — Kinesis MD5-hashes it internally to place
    the record into a shard's hash-key range; the producer applies no hash
    of its own. neighborhood is never None for this project's fixed segment
    list, but `or ""` keeps the key stable if that ever changes."""
    return (
        f"{segment.city}|{segment.neighborhood or ''}|"
        f"{segment.property_type}|{segment.bedrooms}"
    )


def publish_batch(
    client: Any,
    stream_name: str,
    records: list[KinesisRecord],
    max_retries: int,
    backoff_base_seconds: float,
) -> None:
    """Send records via put_records, resending unmodified (same bytes, same
    event_id — never regenerated) any records that fail, up to max_retries
    with exponential backoff. Records still failing after retries are logged
    and dropped rather than blocking the tick or crashing the process — see
    specs/phases/03-market-ingestion/spec.md §4, Failure handling on publish,
    for the at-least-once-delivery rationale."""
    pending: list[KinesisRecord] = records
    attempt = 0

    while pending:
        response = client.put_records(StreamName=stream_name, Records=pending)
        failed = [
            pending[i]
            for i, entry in enumerate(response["Records"])
            if "ErrorCode" in entry
        ]
        if not failed:
            return

        if attempt >= max_retries:
            for i, entry in enumerate(response["Records"]):
                if "ErrorCode" in entry:
                    logger.warning(
                        "publish_failed",
                        partition_key=pending[i]["PartitionKey"],
                        error_code=entry["ErrorCode"],
                    )
            return

        time.sleep(backoff_base_seconds * (2**attempt))
        pending = failed
        attempt += 1
