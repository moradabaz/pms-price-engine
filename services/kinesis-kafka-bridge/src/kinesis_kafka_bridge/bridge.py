from typing import Any

from common import get_logger

logger = get_logger(__name__)


def list_shard_ids(client: Any, stream_name: str) -> list[str]:
    """Lists every shard of a Kinesis stream. Returns their shard IDs."""
    shards = client.list_shards(StreamName=stream_name)["Shards"]
    return [s["ShardId"] for s in shards]


def latest_iterator(client: Any, stream_name: str, shard_id: str) -> str:
    """Gets a LATEST shard iterator — the bridge only forwards new records
    from the moment it starts, never backfills. Returns the iterator."""
    resp = client.get_shard_iterator(
        StreamName=stream_name, ShardId=shard_id, ShardIteratorType="LATEST"
    )
    return resp["ShardIterator"]


def poll_shard(client: Any, shard_iterator: str) -> tuple[list[dict], str | None]:
    """Reads available records from one shard iterator. Returns (records,
    next_iterator) — next_iterator is None if the shard has closed."""
    resp = client.get_records(ShardIterator=shard_iterator, Limit=500)
    return resp["Records"], resp.get("NextShardIterator")


def republish(producer: Any, topic: str, records: list[dict]) -> int:
    """Republishes each Kinesis record's raw bytes to Kafka unmodified — same
    event_id, same payload, never regenerated (at-least-once, not a new
    event). Returns the count republished."""
    for record in records:
        producer.send(topic, key=record["PartitionKey"].encode("utf-8"), value=record["Data"])
    if records:
        producer.flush()
    return len(records)
