from typing import Any


def get_checkpoint(client: Any, table_name: str, shard_id: str) -> str | None:
    """Reads the last successfully processed sequence_number for a shard.
    Returns None if this shard has never been checkpointed."""
    resp = client.get_item(
        TableName=table_name, Key={"shard_id": {"S": shard_id}}
    )
    item = resp.get("Item")
    return item["sequence_number"]["S"] if item else None


def put_checkpoint(
    client: Any, table_name: str, shard_id: str, sequence_number: str
) -> None:
    """Writes a shard's checkpoint. Called only after the Iceberg merge for
    that batch has already succeeded (spec 05 §5) — a crash between the merge
    and this write re-reads (and re-merges, harmlessly, by decision_id) the
    same records rather than skipping them."""
    client.put_item(
        TableName=table_name,
        Item={
            "shard_id": {"S": shard_id},
            "sequence_number": {"S": sequence_number},
        },
    )
