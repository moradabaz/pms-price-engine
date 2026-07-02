"""Contract tests for payment_line.v1 — the CDC output of Debezium reading
public.payment_lines (see specs/phases/02-cdc-pipeline/spec.md).

Beyond plain schema conformance, this suite encodes the decisions in
ADR-0003 as regression tests: a message with leaked CDC envelope metadata, or
with the pre-ADR-0003 nested supplier/billing_period shape, must fail
validation. If either of those ever passes again, the CDC contract has
silently regressed.
"""

import jsonschema
import pytest
from conftest import load_fixture, load_schema


@pytest.fixture
def schema() -> dict:
    return load_schema("payment_line.v1.json")


def validate(schema: dict, instance: dict) -> None:
    jsonschema.Draft202012Validator(schema).validate(instance)


def test_valid_insert_conforms(schema):
    validate(schema, load_fixture("payment_line", "valid_insert.json"))


def test_valid_update_conforms(schema):
    validate(schema, load_fixture("payment_line", "valid_update.json"))


def test_update_reuses_insert_event_id():
    """ADR-0003, point 2: event_id identifies a row, not a message — an
    update must carry the same event_id as the original insert, and its
    updated_at must move forward."""
    inserted = load_fixture("payment_line", "valid_insert.json")
    updated = load_fixture("payment_line", "valid_update.json")
    assert inserted["event_id"] == updated["event_id"]
    assert updated["updated_at"] > inserted["created_at"]


def test_missing_required_field_rejected(schema):
    with pytest.raises(jsonschema.ValidationError):
        validate(schema, load_fixture("payment_line", "invalid_missing_required.json"))


def test_cdc_envelope_metadata_rejected(schema):
    """ADR-0003, point 1: leaked Debezium envelope fields (__op,
    __source_ts_ms, ...) must fail validation — the Kafka payload is the
    pure business event only, per infra/debezium/postgres-connector.json no
    longer setting transforms.unwrap.add.fields."""
    with pytest.raises(jsonschema.ValidationError):
        validate(schema, load_fixture("payment_line", "invalid_cdc_metadata_leak.json"))


def test_legacy_nested_shape_rejected(schema):
    """ADR-0003, point 3: the pre-ADR-0003 nested supplier/billing_period
    shape must fail against the flattened schema — regression guard against
    ever reverting the flattening decision."""
    with pytest.raises(jsonschema.ValidationError):
        validate(
            schema, load_fixture("payment_line", "invalid_legacy_nested_supplier.json")
        )
