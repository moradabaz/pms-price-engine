import json

from flink_jobs.job import _parse_apartment_segment_row

_BASE_ROW = {
    "apartment_id": "BCN-001",
    "city": "Barcelona",
    "neighborhood": "Eixample",
    "property_type": "studio",
    "bedrooms": 0,
    "target_margin": 0.05,
    "competitiveness_discount": 0.05,
}


def test_parses_commission_pct_when_present():
    row = _parse_apartment_segment_row(
        json.dumps({**_BASE_ROW, "commission_pct": 0.2})
    )
    assert row.commission_pct == 0.2


def test_defaults_commission_pct_for_historical_messages():
    # ADR-0009: commission_pct was added to apartment_market_segments after
    # this topic already had history — replaying from the earliest offset
    # (job.py's segment_source) surfaces messages predating the column.
    row = _parse_apartment_segment_row(json.dumps(_BASE_ROW))
    assert row.commission_pct == 0.15
