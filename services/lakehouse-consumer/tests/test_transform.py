from datetime import UTC, datetime

from lakehouse_consumer.transform import row_from_new_image


def test_row_from_new_image_converts_types(sample_new_image):
    image = sample_new_image("11111111-1111-1111-1111-111111111111", 132.0)
    ingested_at = datetime(2026, 8, 4, 10, 0, 5, tzinfo=UTC)

    row = row_from_new_image(image, "INSERT", ingested_at)

    assert row["target_date"].isoformat() == "2026-09-01"
    assert row["decided_at"].year == 2026
    assert row["cost_inputs"]["billing_period"]["start"].isoformat() == "2026-08-01"
    assert row["market_inputs"]["avg_nightly_rate_eur"] == 145.0
    assert row["dynamodb_event_name"] == "INSERT"
    assert row["ingested_at"] == ingested_at


def test_row_from_new_image_defaults_missing_los_floor_matrix_to_empty_list(
    sample_new_image,
):
    # Regression test: a real DynamoDB Streams record predating Phase 9
    # (ADR-0011 backlog #1) has no calculation.los_floor_matrix key at all —
    # caught live against LocalStack as a KeyError, not by the unit suite.
    image = sample_new_image("22222222-2222-2222-2222-222222222222", 132.0)
    del image["calculation"]["los_floor_matrix"]

    ingested_at = datetime(2026, 8, 4, 10, 0, 5, tzinfo=UTC)
    row = row_from_new_image(image, "INSERT", ingested_at)

    assert row["calculation"]["los_floor_matrix"] == []


def test_row_from_new_image_defaults_missing_property_fields_pre_phase_8(
    sample_new_image,
):
    # A record predating Phase 8 (ADR-0011 backlog #6) has neither
    # property_attribute_factor nor property_reference_price_eur —
    # property_reference_price_eur must default to avg_nightly_rate_eur
    # (the true pre-Phase-8 value, since the factor's own default is 1.0).
    image = sample_new_image("33333333-3333-3333-3333-333333333333", 132.0)
    del image["calculation"]["property_attribute_factor"]
    del image["calculation"]["property_reference_price_eur"]

    ingested_at = datetime(2026, 8, 4, 10, 0, 5, tzinfo=UTC)
    row = row_from_new_image(image, "INSERT", ingested_at)

    assert row["calculation"]["property_attribute_factor"] == 1.0
    assert row["calculation"]["property_reference_price_eur"] == (
        row["market_inputs"]["avg_nightly_rate_eur"]
    )
