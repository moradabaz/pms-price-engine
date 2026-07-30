"""Contract tests for price_decision.v1 — produced by the Flink pricing
engine (Phase 4) and written to both DynamoDB and Iceberg."""

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


def test_valid_minimum_floor_decision_conforms(schema):
    # rule_applied=minimum_floor: the cost floor won, but stays at/below
    # avg_nightly_rate_eur — still competitive against the raw market average
    # (ADR-0007).
    validate(schema, load_fixture("price_decision", "valid_minimum_floor.json"))


def test_valid_cost_protected_decision_conforms(schema):
    # rule_applied=cost_protected: the cost floor pushed the price above
    # avg_nightly_rate_eur — below_market_by is negative by design (ADR-0007).
    validate(schema, load_fixture("price_decision", "valid_cost_protected.json"))


def test_missing_required_field_rejected(schema):
    with pytest.raises(jsonschema.ValidationError):
        validate(
            schema, load_fixture("price_decision", "invalid_missing_required.json")
        )
