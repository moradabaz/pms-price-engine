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


def test_row_from_new_image_defaults_missing_decision_components_to_empty_list(
    sample_new_image,
):
    # Regression test: a record predating Phase 10 (ADR-0011 backlog #4) has
    # no decision_components at either nesting level — empty list is the
    # honest answer, same convention Phase 9 established for los_floor_matrix
    # itself.
    image = sample_new_image("44444444-4444-4444-4444-444444444444", 132.0)
    del image["calculation"]["decision_components"]
    del image["calculation"]["los_floor_matrix"][0]["decision_components"]

    ingested_at = datetime(2026, 8, 4, 10, 0, 5, tzinfo=UTC)
    row = row_from_new_image(image, "INSERT", ingested_at)

    assert row["calculation"]["decision_components"] == []
    assert row["calculation"]["los_floor_matrix"][0]["decision_components"] == []


def test_row_from_new_image_defaults_missing_commission_base_to_total_revenue(
    sample_new_image,
):
    # Regression test: a record predating Phase 11 (ADR-0011 backlog #5) has
    # no commission_base at all — total_revenue is the true pre-Phase-11
    # value (decide_price()'s own default), not a fabricated one.
    image = sample_new_image("55555555-5555-5555-5555-555555555555", 132.0)
    del image["calculation"]["commission_base"]

    ingested_at = datetime(2026, 8, 4, 10, 0, 5, tzinfo=UTC)
    row = row_from_new_image(image, "INSERT", ingested_at)

    assert row["calculation"]["commission_base"] == "total_revenue"


def test_row_from_new_image_derives_missing_floor_policy_from_floor_type_soft(
    sample_new_image,
):
    # Regression test: a record predating Phase 12 (ADR-0011 backlog #10) has
    # no floor_policy at all — re-derived from floor_type, never a fabricated
    # constant (spec 12 §D). structural_full_margin -> soft.
    image = sample_new_image("66666666-6666-6666-6666-666666666666", 132.0)
    del image["calculation"]["floor_policy"]
    del image["calculation"]["los_floor_matrix"][0]["floor_policy"]

    ingested_at = datetime(2026, 8, 4, 10, 0, 5, tzinfo=UTC)
    row = row_from_new_image(image, "INSERT", ingested_at)

    assert row["calculation"]["floor_policy"] == "soft"
    assert row["calculation"]["los_floor_matrix"][0]["floor_policy"] == "soft"


def test_row_from_new_image_derives_missing_floor_policy_from_floor_type_hard(
    sample_new_image,
):
    # Same fallback, but for the contribution -> hard mapping.
    image = sample_new_image("77777777-7777-7777-7777-777777777777", 132.0)
    image["calculation"]["floor_type"] = "contribution"
    image["calculation"]["los_floor_matrix"][0]["floor_type"] = "contribution"
    del image["calculation"]["floor_policy"]
    del image["calculation"]["los_floor_matrix"][0]["floor_policy"]

    ingested_at = datetime(2026, 8, 4, 10, 0, 5, tzinfo=UTC)
    row = row_from_new_image(image, "INSERT", ingested_at)

    assert row["calculation"]["floor_policy"] == "hard"
    assert row["calculation"]["los_floor_matrix"][0]["floor_policy"] == "hard"
