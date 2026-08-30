from datetime import date

import pytest
from flink_jobs.cost_aggregation import aggregate_cost, retained_billing_period_ends
from shared_schemas.payment_line import PaymentLine


def _line(
    event_id,
    amount,
    period_start,
    period_end,
    cost_type="variable",
    concept="electricity",
):
    return PaymentLine.model_validate(
        {
            "event_id": event_id,
            "schema_version": "1.0",
            "apartment_id": "BCN-001",
            "apartment_reference": "BCN-001",
            "concept": concept,
            "cost_type": cost_type,
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
    assert result.variable_cost_eur == 10.0


def test_cost_type_split_fixed_variable_one_time():
    lines = [
        _line(
            "00000000-0000-0000-0000-000000000001",
            300.0,
            "2026-06-01",
            "2026-06-30",
            cost_type="fixed",
        ),
        _line(
            "00000000-0000-0000-0000-000000000002",
            60.0,
            "2026-06-01",
            "2026-06-30",
            cost_type="variable",
        ),
        _line(
            "00000000-0000-0000-0000-000000000003",
            70.0,
            "2026-06-01",
            "2026-06-30",
            cost_type="one_time",
        ),
    ]
    result = aggregate_cost(lines)
    assert result.fixed_cost_eur == 10.0  # 300 / 30 days
    assert result.variable_cost_eur == 2.0  # 60 / 30 days


def test_one_time_cost_is_averaged_not_summed():
    # Two cleaning invoices in the same period — each already the cost of one
    # turnover; the floor needs one representative value, not their sum
    # (ADR-0009 D3).
    lines = [
        _line(
            "00000000-0000-0000-0000-000000000001",
            60.0,
            "2026-06-01",
            "2026-06-30",
            cost_type="one_time",
        ),
        _line(
            "00000000-0000-0000-0000-000000000002",
            80.0,
            "2026-06-01",
            "2026-06-30",
            cost_type="one_time",
        ),
    ]
    result = aggregate_cost(lines)
    assert result.one_time_cost_eur == 70.0  # average, not 140.0


def test_no_one_time_lines_yields_zero():
    lines = [
        _line("00000000-0000-0000-0000-000000000001", 100.0, "2026-06-01", "2026-06-30")
    ]
    result = aggregate_cost(lines)
    assert result.one_time_cost_eur == 0.0


def test_ota_related_and_cleaning_sub_totals_are_split_from_concept():
    # Phase 11 (ADR-0011 backlog #5, spec 11 §4): the minimal slice of
    # backlog #3 this phase needs — ota_fee/channel_manager -> ota_related,
    # cleaning -> cleaning, both still fully counted in fixed/variable above.
    lines = [
        _line(
            "00000000-0000-0000-0000-000000000001",
            300.0,
            "2026-06-01",
            "2026-06-30",
            cost_type="variable",
            concept="ota_fee",
        ),
        _line(
            "00000000-0000-0000-0000-000000000002",
            150.0,
            "2026-06-01",
            "2026-06-30",
            cost_type="fixed",
            concept="channel_manager",
        ),
        _line(
            "00000000-0000-0000-0000-000000000003",
            120.0,
            "2026-06-01",
            "2026-06-30",
            cost_type="variable",
            concept="cleaning",
        ),
        _line(
            "00000000-0000-0000-0000-000000000004",
            60.0,
            "2026-06-01",
            "2026-06-30",
            cost_type="variable",
            concept="electricity",
        ),
    ]
    result = aggregate_cost(lines)
    assert result.ota_related_cost_eur == 15.0  # (300 + 150) / 30 days
    assert result.cleaning_cost_eur == 4.0  # 120 / 30 days
    # Still fully counted in the existing cost_type totals — not removed.
    assert result.fixed_cost_eur == 5.0  # 150 / 30
    assert result.variable_cost_eur == 16.0  # (300 + 120 + 60) / 30


def test_no_ota_or_cleaning_lines_yields_zero_sub_totals():
    lines = [
        _line("00000000-0000-0000-0000-000000000001", 100.0, "2026-06-01", "2026-06-30")
    ]
    result = aggregate_cost(lines)
    assert result.ota_related_cost_eur == 0.0
    assert result.cleaning_cost_eur == 0.0


def test_retained_billing_period_ends_keeps_top_two():
    ends = [date(2026, 6, 30), date(2026, 5, 31), date(2026, 4, 30)]
    assert retained_billing_period_ends(ends) == {date(2026, 6, 30), date(2026, 5, 31)}


@pytest.mark.parametrize("keep", [1, 3])
def test_retained_billing_period_ends_respects_keep(keep):
    ends = [date(2026, 6, 30), date(2026, 5, 31), date(2026, 4, 30)]
    assert len(retained_billing_period_ends(ends, keep=keep)) == min(keep, len(ends))
