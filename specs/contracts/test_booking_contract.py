"""Contract tests for booking.v1 — the CDC output of Debezium reading
public.bookings (Phase 18, ADR-0011 backlog #13 prerequisite).
"""

import jsonschema
import pytest
from conftest import load_fixture, load_schema


@pytest.fixture
def schema() -> dict:
    return load_schema("booking.v1.json")


def validate(schema: dict, instance: dict) -> None:
    jsonschema.Draft202012Validator(schema).validate(instance)


def test_valid_insert_conforms(schema):
    validate(schema, load_fixture("booking", "valid_insert.json"))


def test_valid_update_conforms(schema):
    validate(schema, load_fixture("booking", "valid_update.json"))


def test_update_reuses_insert_booking_id():
    """booking_id identifies a row, not a message — a status update (e.g. a
    cancellation) must carry the same booking_id as the original insert."""
    inserted = load_fixture("booking", "valid_insert.json")
    updated = load_fixture("booking", "valid_update.json")
    assert inserted["booking_id"] == updated["booking_id"]
    assert updated["updated_at"] > inserted["created_at"]


def test_missing_required_field_rejected(schema):
    with pytest.raises(jsonschema.ValidationError):
        validate(schema, load_fixture("booking", "invalid_missing_required.json"))
