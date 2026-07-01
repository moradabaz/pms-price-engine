"""Contract tests for market_price.v1 — published by the market-ingestor
service (Phase 2) to the market-price-events Kinesis stream."""

import jsonschema
import pytest
from conftest import load_fixture, load_schema


@pytest.fixture
def schema() -> dict:
    return load_schema("market_price.v1.json")


def validate(schema: dict, instance: dict) -> None:
    jsonschema.Draft202012Validator(schema).validate(instance)


def test_valid_snapshot_conforms(schema):
    validate(schema, load_fixture("market_price", "valid_snapshot.json"))


def test_missing_required_field_rejected(schema):
    with pytest.raises(jsonschema.ValidationError):
        validate(schema, load_fixture("market_price", "invalid_missing_required.json"))
