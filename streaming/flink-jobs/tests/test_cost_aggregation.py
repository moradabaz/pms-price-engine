from datetime import date

import pytest
from flink_jobs.cost_aggregation import aggregate_cost, retained_billing_period_ends
from shared_schemas.payment_line import PaymentLine


def _line(event_id, amount, period_start, period_end):
    return PaymentLine.model_validate(
        {
            "event_id": event_id,
            "schema_version": "1.0",
            "apartment_id": "BCN-001",
            "apartment_reference": "BCN-001",
            "concept": "electricity",
            "cost_type": "variable",
            "description": "test",
            "amount_gross": amount,
            "vat_rate": 0.21,
            "currency": "EUR",
            "billing_period_start": period_start,
            "billing_period_end": period_end,
            "payment_status": "paid",
            "source": "synthetic",
            "created_at": "2026-07-01T00:00:00Z",
        }
    )


def test_empty_returns_none():
    assert aggregate_cost([]) is None


def test_sums_only_current_period():
    lines = [
        _line(
            "00000000-0000-0000-0000-000000000001", 100.0, "2026-06-01", "2026-06-30"
        ),
        _line("00000000-0000-0000-0000-000000000002", 50.0, "2026-06-01", "2026-06-30"),
        _line(
            "00000000-0000-0000-0000-000000000003", 999.0, "2026-05-01", "2026-05-31"
        ),
    ]
    result = aggregate_cost(lines)
    assert result.total_monthly_cost_eur == 150.0
    assert result.cost_lines_count == 2
    assert result.billing_period_end == date(2026, 6, 30)


def test_available_days_is_calendar_length():
    lines = [
        _line("00000000-0000-0000-0000-000000000001", 300.0, "2026-06-01", "2026-06-30")
    ]
    result = aggregate_cost(lines)
    assert result.available_days == 30
    assert result.daily_cost_eur == 10.0


def test_retained_billing_period_ends_keeps_top_two():
    ends = [date(2026, 6, 30), date(2026, 5, 31), date(2026, 4, 30)]
    assert retained_billing_period_ends(ends) == {date(2026, 6, 30), date(2026, 5, 31)}


@pytest.mark.parametrize("keep", [1, 3])
def test_retained_billing_period_ends_respects_keep(keep):
    ends = [date(2026, 6, 30), date(2026, 5, 31), date(2026, 4, 30)]
    assert len(retained_billing_period_ends(ends, keep=keep)) == min(keep, len(ends))
