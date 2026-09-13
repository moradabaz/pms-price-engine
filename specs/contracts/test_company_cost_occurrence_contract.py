"""Contract tests for company_cost_occurrence.v1 (Phase 19, ADR-0011 backlog
#13) — the CDC output of Debezium reading public.company_cost_occurrences.
"""

import jsonschema
import pytest
from conftest import load_fixture, load_schema


@pytest.fixture
def schema() -> dict:
    return load_schema("company_cost_occurrence.v1.json")


def validate(schema: dict, instance: dict) -> None:
    jsonschema.Draft202012Validator(schema).validate(instance)


def test_valid_insert_conforms(schema):
    validate(schema, load_fixture("company_cost_occurrence", "valid_insert.json"))


def test_missing_required_field_rejected(schema):
    with pytest.raises(jsonschema.ValidationError):
        validate(
            schema,
            load_fixture("company_cost_occurrence", "invalid_missing_required.json"),
        )
