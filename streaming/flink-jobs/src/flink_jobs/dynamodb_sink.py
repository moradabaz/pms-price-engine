import time
from typing import Any

import boto3
from pyflink.datastream.functions import MapFunction
from shared_schemas.price_decision import PriceDecision


class DynamoDbSinkFunction(MapFunction):
    """Writes each PriceDecision to DynamoDB via put_item, keyed by
    apartment_id/target_date. Pass-through map (no native Python Sink API
    in this PyFlink version) — chain with a DiscardingSink downstream."""

    def __init__(
        self,
        table_name: str,
        endpoint_url: str | None,
        region_name: str,
        max_retries: int = 3,
    ):
        self.table_name = table_name
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.max_retries = max_retries

    def open(self, runtime_context):
        self.client = boto3.client(
            "dynamodb", region_name=self.region_name, endpoint_url=self.endpoint_url
        )

    def map(self, value: PriceDecision) -> PriceDecision:
        item = _to_dynamodb_item(value)
        attempt = 0
        while True:
            try:
                self.client.put_item(TableName=self.table_name, Item=item)
                return value
            except Exception:
                attempt += 1
                if attempt > self.max_retries:
                    raise
                time.sleep(0.5 * (2**attempt))


def _to_dynamodb_item(decision: PriceDecision) -> dict[str, Any]:
    """Builds a DynamoDB item from a PriceDecision. Returns the item dict.
    put_item's Item is the bare attribute map — not wrapped in {"M": ...}
    the way a nested attribute value would be."""
    data = decision.model_dump(mode="json")
    return _python_to_dynamodb(data)["M"]


def _python_to_dynamodb(value: Any) -> Any:
    """Converts a plain Python value into DynamoDB's typed attribute format."""
    if isinstance(value, str):
        return {"S": value}
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, int | float):
        return {"N": str(value)}
    if isinstance(value, dict):
        return {"M": {k: _python_to_dynamodb(v) for k, v in value.items()}}
    if value is None:
        return {"NULL": True}
    raise TypeError(f"Unsupported type for DynamoDB item: {type(value)}")
