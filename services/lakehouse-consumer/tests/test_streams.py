import pytest
from lakehouse_consumer.streams import (
    deserialize_image,
    iterator_type_for,
    order_shards_for_processing,
    parse_stream_record,
    resolve_stream_arn,
)


class FakeDynamoDbClient:
    def __init__(self, stream_arn: str | None):
        self._stream_arn = stream_arn

    def describe_table(self, TableName: str) -> dict:  # noqa: N803
        table: dict = {"TableName": TableName}
        if self._stream_arn:
            table["LatestStreamArn"] = self._stream_arn
        return {"Table": table}


class FakeStreamsClient:
    def __init__(self, registered_arns: list[str]):
        self._registered_arns = registered_arns

    def list_streams(self, TableName: str) -> dict:  # noqa: N803
        return {"Streams": [{"StreamArn": arn} for arn in self._registered_arns]}


_ARN_PREFIX = "arn:aws:dynamodb:eu-west-1:000000000000:table/price_decision/stream"


def test_resolve_stream_arn_returns_registered_arn():
    arn = f"{_ARN_PREFIX}/2026-08-04T00:00:00.000"
    dynamodb = FakeDynamoDbClient(arn)
    streams = FakeStreamsClient([arn])

    assert resolve_stream_arn(dynamodb, streams, "price_decision") == arn


def test_resolve_stream_arn_raises_when_not_registered():
    # Live-verified LocalStack failure mode (pre-spec Decision F): describe_table
    # reports an ARN from a previous container lifetime that list_streams no
    # longer recognizes.
    dynamodb = FakeDynamoDbClient(f"{_ARN_PREFIX}/stale")
    streams = FakeStreamsClient([])

    with pytest.raises(RuntimeError, match="not a registered stream"):
        resolve_stream_arn(dynamodb, streams, "price_decision")


def test_resolve_stream_arn_raises_when_streaming_disabled():
    dynamodb = FakeDynamoDbClient(None)
    streams = FakeStreamsClient([])

    with pytest.raises(RuntimeError, match="no stream enabled"):
        resolve_stream_arn(dynamodb, streams, "price_decision")


def test_order_shards_defers_child_until_parent_drained():
    shards = [
        {"ShardId": "parent-1"},
        {"ShardId": "child-1", "ParentShardId": "parent-1"},
    ]

    # Parent not yet drained: only the parent is ready.
    ready = order_shards_for_processing(shards, drained_shard_ids=set())
    assert [s["ShardId"] for s in ready] == ["parent-1"]

    # Parent drained: the child is now ready too.
    ready = order_shards_for_processing(shards, drained_shard_ids={"parent-1"})
    assert {s["ShardId"] for s in ready} == {"parent-1", "child-1"}


def test_order_shards_treats_expired_parent_as_safe():
    # The parent shard has already aged out of the stream's shard list
    # entirely (real AWS behavior) — nothing left to drain, so its child is
    # safe to read even though drained_shard_ids never saw the parent.
    shards = [{"ShardId": "child-1", "ParentShardId": "parent-gone"}]

    ready = order_shards_for_processing(shards, drained_shard_ids=set())

    assert [s["ShardId"] for s in ready] == ["child-1"]


def test_order_shards_localstack_single_shard_case():
    # Live-verified LocalStack behavior (pre-spec Decision F): exactly one
    # shard, always open, no ParentShardId.
    shards = [{"ShardId": "shardId-000000000000"}]

    ready = order_shards_for_processing(shards, drained_shard_ids=set())

    assert [s["ShardId"] for s in ready] == ["shardId-000000000000"]


@pytest.mark.parametrize(
    ("checkpoint", "expected"),
    [(None, "TRIM_HORIZON"), ("12345", "AFTER_SEQUENCE_NUMBER")],
)
def test_iterator_type_for(checkpoint, expected):
    assert iterator_type_for(checkpoint) == expected


def test_deserialize_image_converts_typed_attributes():
    image = {
        "apartment_id": {"S": "BCN-001"},
        "decided_at": {"S": "2026-08-04T10:00:00+00:00"},
        "calculation": {
            "M": {
                "target_margin": {"N": "0.05"},
                "rule_applied": {"S": "market_competitive"},
            }
        },
    }

    result = deserialize_image(image)

    assert result["apartment_id"] == "BCN-001"
    assert result["calculation"]["rule_applied"] == "market_competitive"
    assert float(result["calculation"]["target_margin"]) == 0.05


def test_deserialize_image_none_for_missing_image():
    assert deserialize_image(None) is None


def test_parse_stream_record_extracts_new_image():
    record = {
        "eventName": "INSERT",
        "dynamodb": {
            "SequenceNumber": "111",
            "NewImage": {"apartment_id": {"S": "BCN-001"}},
        },
    }

    parsed = parse_stream_record(record)

    assert parsed == {
        "event_name": "INSERT",
        "sequence_number": "111",
        "new_image": {"apartment_id": "BCN-001"},
    }
