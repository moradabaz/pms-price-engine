from lakehouse_consumer.checkpoint import get_checkpoint, put_checkpoint


class FakeCheckpointClient:
    """In-memory stand-in for boto3's DynamoDB client, typed-attribute shaped
    exactly like the real one (spec 05 §10 — checkpoint table, PK shard_id)."""

    def __init__(self):
        self._items: dict[str, dict] = {}

    def get_item(self, TableName: str, Key: dict) -> dict:  # noqa: N803
        shard_id = Key["shard_id"]["S"]
        item = self._items.get(shard_id)
        return {"Item": item} if item else {}

    def put_item(self, TableName: str, Item: dict) -> None:  # noqa: N803
        self._items[Item["shard_id"]["S"]] = Item


def test_get_checkpoint_returns_none_when_absent():
    client = FakeCheckpointClient()

    assert get_checkpoint(client, "stream_checkpoints", "shard-1") is None


def test_put_then_get_checkpoint_round_trips():
    client = FakeCheckpointClient()

    put_checkpoint(client, "stream_checkpoints", "shard-1", "999")

    assert get_checkpoint(client, "stream_checkpoints", "shard-1") == "999"


def test_put_checkpoint_overwrites_previous_value():
    client = FakeCheckpointClient()
    put_checkpoint(client, "stream_checkpoints", "shard-1", "100")

    put_checkpoint(client, "stream_checkpoints", "shard-1", "200")

    assert get_checkpoint(client, "stream_checkpoints", "shard-1") == "200"
