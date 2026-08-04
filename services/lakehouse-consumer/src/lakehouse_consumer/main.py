import time
from datetime import UTC, datetime
from typing import Any

import boto3
from common import configure_logging, get_logger
from lakehouse_shared import build_catalog
from pyiceberg.table import Table

from lakehouse_consumer.checkpoint import get_checkpoint, put_checkpoint
from lakehouse_consumer.iceberg_writer import ensure_table, merge_rows
from lakehouse_consumer.settings import ConsumerSettings
from lakehouse_consumer.streams import (
    iterator_type_for,
    order_shards_for_processing,
    parse_stream_record,
    resolve_stream_arn,
)
from lakehouse_consumer.transform import row_from_new_image


def process_shard_once(
    streams_client: Any,
    dynamodb_client: Any,
    table: Table,
    stream_arn: str,
    shard: dict[str, Any],
    settings: ConsumerSettings,
) -> bool:
    """Reads and merges whatever is currently available on one shard, then
    checkpoints. Returns True if the shard is closed and fully drained (no
    more records, no NextShardIterator) — the caller uses this to unblock
    that shard's children (spec 05 §5, parent-before-child ordering)."""
    logger = get_logger(__name__)
    shard_id = shard["ShardId"]
    checkpoint = get_checkpoint(
        dynamodb_client, settings.checkpoint_table_name, shard_id
    )
    iterator = streams_client.get_shard_iterator(
        StreamArn=stream_arn,
        ShardId=shard_id,
        ShardIteratorType=iterator_type_for(checkpoint),
        **({"SequenceNumber": checkpoint} if checkpoint else {}),
    )["ShardIterator"]

    resp = streams_client.get_records(ShardIterator=iterator, Limit=1000)
    records = resp["Records"]
    if records:
        parsed = [parse_stream_record(r) for r in records]
        ingested_at = datetime.now(UTC)
        rows = [
            row_from_new_image(p["new_image"], p["event_name"], ingested_at)
            for p in parsed
            if p["new_image"] is not None
        ]
        merge_rows(table, rows)
        last_sequence_number = parsed[-1]["sequence_number"]
        put_checkpoint(
            dynamodb_client,
            settings.checkpoint_table_name,
            shard_id,
            last_sequence_number,
        )
        logger.info("shard_batch_merged", shard_id=shard_id, count=len(rows))

    return resp.get("NextShardIterator") is None


def run_forever(
    streams_client: Any,
    dynamodb_client: Any,
    table: Table,
    settings: ConsumerSettings,
) -> None:
    logger = get_logger(__name__)
    stream_arn = resolve_stream_arn(
        dynamodb_client, streams_client, settings.source_table_name
    )
    logger.info("consumer_starting", stream_arn=stream_arn)
    drained_shard_ids: set[str] = set()

    while True:
        stream_description = streams_client.describe_stream(StreamArn=stream_arn)
        shards = stream_description["StreamDescription"]["Shards"]
        for shard in order_shards_for_processing(shards, drained_shard_ids):
            if process_shard_once(
                streams_client, dynamodb_client, table, stream_arn, shard, settings
            ):
                drained_shard_ids.add(shard["ShardId"])
        time.sleep(settings.poll_interval_seconds)


def main() -> None:
    settings = ConsumerSettings()
    configure_logging(settings.log_level)

    dynamodb_client = boto3.client(
        "dynamodb",
        region_name=settings.aws_region,
        endpoint_url=settings.dynamodb_endpoint_url,
    )
    streams_client = boto3.client(
        "dynamodbstreams",
        region_name=settings.aws_region,
        endpoint_url=settings.dynamodb_endpoint_url,
    )
    catalog = build_catalog(settings)
    table = ensure_table(catalog, settings)

    run_forever(streams_client, dynamodb_client, table, settings)


if __name__ == "__main__":
    main()
