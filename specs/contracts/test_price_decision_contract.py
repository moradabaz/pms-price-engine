"""Contract tests for price_decision.v1 — produced by the Flink pricing
engine (Phase 3) and written to both DynamoDB and Iceberg."""

import jsonschema
import pytest
from conftest import load_fixture, load_schema


@pytest.fixture
def schema() -> dict:
    return load_schema("price_decision.v1.json")


def validate(schema: dict, instance: dict) -> None:
    jsonschema.Draft202012Validator(schema).validate(instance)


def test_valid_market_competitive_decision_conforms(schema):
    validate(schema, load_fixture("price_decision", "valid_market_competitive.json"))


def test_missing_required_field_rejected(schema):
    with pytest.raises(jsonschema.ValidationError):
        validate(
            schema, load_fixture("price_decision", "invalid_missing_required.json")
        )
