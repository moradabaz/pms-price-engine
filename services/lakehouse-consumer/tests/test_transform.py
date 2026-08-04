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
