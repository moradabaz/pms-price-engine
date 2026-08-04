from typing import Any, Literal

from boto3.dynamodb.types import TypeDeserializer

_deserializer = TypeDeserializer()


def resolve_stream_arn(
    dynamodb_client: Any, streams_client: Any, table_name: str
) -> str:
    """Resolves a table's stream ARN dynamically — never hardcode it (spec 05
    §5). Live-verified against LocalStack (pre-spec Decision F): the ARN does
    not reliably survive a container restart even though the table's item
    data does, so describe_table alone isn't trustworthy — confirm the ARN is
    actually registered via list_streams before using it. Returns the ARN."""
    table = dynamodb_client.describe_table(TableName=table_name)["Table"]
    stream_arn = table.get("LatestStreamArn")
    if not stream_arn:
        raise RuntimeError(f"Table {table_name!r} has no stream enabled")

    registered = {
        s["StreamArn"]
        for s in streams_client.list_streams(TableName=table_name).get("Streams", [])
    }
    if stream_arn not in registered:
        raise RuntimeError(
            f"Stream {stream_arn!r} from describe_table is not a registered "
            f"stream for {table_name!r} (registered: {registered or 'none'})"
        )
    return str(stream_arn)


def order_shards_for_processing(
    shards: list[dict[str, Any]], drained_shard_ids: set[str]
) -> list[dict[str, Any]]:
    """Orders shards for reading, honoring DynamoDB Streams' parent-before-child
    rule (a real AWS ordering requirement — a child shard's records must not be
    read before its parent's are fully drained). A shard whose parent is still
    present in `shards` and not yet in `drained_shard_ids` is deferred; a shard
    whose parent has already expired out of the shard list entirely is treated
    as safe to read (nothing left to drain). Returns the shards safe to read
    this pass, in parent-first order for shards sharing a lineage."""
    shard_ids_present = {s["ShardId"] for s in shards}
    ready = []
    for shard in shards:
        parent_id = shard.get("ParentShardId")
        parent_pending = (
            parent_id is not None
            and parent_id in shard_ids_present
            and parent_id not in drained_shard_ids
        )
        if not parent_pending:
            ready.append(shard)
    ready.sort(key=lambda s: (s.get("ParentShardId") is not None, s["ShardId"]))
    return ready


def iterator_type_for(
    checkpoint: str | None,
) -> Literal["TRIM_HORIZON", "AFTER_SEQUENCE_NUMBER"]:
    """A shard with no checkpoint yet reads from the start of its retained
    history; one with a checkpoint resumes strictly after it (never re-reads
    the last committed record). Returns the iterator type to request."""
    return "AFTER_SEQUENCE_NUMBER" if checkpoint else "TRIM_HORIZON"


def deserialize_image(image: dict[str, Any] | None) -> dict[str, Any] | None:
    """Converts a DynamoDB Streams NewImage/OldImage typed-attribute map
    (e.g. {"apartment_id": {"S": "BCN-001"}}) into plain Python values.
    Returns None if the image is absent (e.g. a REMOVE record with only
    OldImage available under a view type that doesn't include NEW_IMAGE)."""
    if image is None:
        return None
    return {key: _deserializer.deserialize(value) for key, value in image.items()}


def parse_stream_record(record: dict[str, Any]) -> dict[str, Any]:
    """Parses one raw DynamoDB Streams record into the shape the Iceberg
    writer needs. Returns event_name, sequence_number, and the deserialized
    new_image (never old_image — price_decision.v1 rows are immutable audit
    entries, only NewImage is ever written downstream)."""
    dynamodb_record = record["dynamodb"]
    return {
        "event_name": record["eventName"],
        "sequence_number": dynamodb_record["SequenceNumber"],
        "new_image": deserialize_image(dynamodb_record.get("NewImage")),
    }
