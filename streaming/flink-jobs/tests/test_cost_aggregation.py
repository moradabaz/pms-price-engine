from datetime import date

import pytest
from flink_jobs.cost_aggregation import (
    ConceptAmount,
    EnrichedPaymentLine,
    PercentageCostComponent,
    aggregate_cost,
    retained_billing_period_ends,
)


def _line(
    event_id,
    amount,
    period_start,
    period_end,
    behavior="variable",
    concept="electricity",
    recurrence="monthly",
    allocation_method="calendar_day",
    scope="property",
    calculation_base="fixed_amount",
    revenue_base=None,
    rate=None,
):
    return EnrichedPaymentLine(
        event_id=event_id,
        apartment_id="BCN-001",
        apartment_reference="BCN-001",
        billing_period_start=date.fromisoformat(period_start),
        billing_period_end=date.fromisoformat(period_end),
        amount_gross=amount,
        concept=concept,
        scope=scope,
        behavior=behavior,
        trigger="time",
        calculation_base=calculation_base,
        recurrence=recurrence,
        revenue_base=revenue_base,
        allocation_method=allocation_method,
        weight_config=None,
        rate=rate,
    )


def _concept_amount(concept, amount_eur, **dims):
    defaults = dict(
        scope="property",
        behavior="variable",
        trigger="time",
        calculation_base="fixed_amount",
        recurrence="monthly",
        allocation_method="calendar_day",
    )
    defaults.update(dims)
    return ConceptAmount(concept=concept, amount_eur=amount_eur, **defaults)


def test_empty_returns_none():
    assert aggregate_cost([]) is None


def test_sums_only_current_period():
    lines = [
        _line("e1", 100.0, "2026-06-01", "2026-06-30"),
        _line("e2", 50.0, "2026-06-01", "2026-06-30"),
        _line("e3", 999.0, "2026-05-01", "2026-05-31"),
    ]
    result = aggregate_cost(lines)
    assert result.total_monthly_cost_eur == 150.0
    assert result.cost_lines_count == 2
    assert result.billing_period_end == date(2026, 6, 30)


def test_available_days_is_calendar_length():
    lines = [_line("e1", 300.0, "2026-06-01", "2026-06-30")]
    result = aggregate_cost(lines)
    assert result.available_days == 30
    assert result.variable_cost_eur == 10.0


def test_behavior_split_fixed_variable_one_off():
    # Phase 19 (ADR-0011 backlog #13): behavior (fixed/variable) plus
    # recurrence=one_off (not a 'one_time' behavior value, which doesn't
    # exist — see ADR-0012's own note on why) drive the split now.
    lines = [
        _line("e1", 300.0, "2026-06-01", "2026-06-30", behavior="fixed"),
        _line("e2", 60.0, "2026-06-01", "2026-06-30", behavior="variable"),
        _line(
            "e3",
            70.0,
            "2026-06-01",
            "2026-06-30",
            behavior="variable",
            recurrence="one_off",
            allocation_method="direct",
        ),
    ]
    result = aggregate_cost(lines)
    assert result.fixed_cost_eur == 10.0  # 300 / 30 days
    assert result.variable_cost_eur == 2.0  # 60 / 30 days


def test_semi_variable_folds_into_variable_bucket():
    lines = [
        _line("e1", 300.0, "2026-06-01", "2026-06-30", behavior="semi_variable"),
    ]
    result = aggregate_cost(lines)
    assert result.variable_cost_eur == 10.0
    assert result.fixed_cost_eur == 0.0


def test_annual_recurrence_is_divided_by_twelve_before_allocation():
    # The "annual lump sum" fix (ADR-0011 backlog #13's own motivating case):
    # a 1200 EUR annual premium becomes 100 EUR/month, then 100/30 per night —
    # not 1200/30, which would spike this one month's fixed_cost_eur 12x.
    lines = [
        _line(
            "e1",
            1200.0,
            "2026-06-01",
            "2026-06-30",
            behavior="fixed",
            recurrence="annual",
        )
    ]
    result = aggregate_cost(lines)
    assert result.fixed_cost_eur == round(100.0 / 30, 2)


def test_one_off_line_is_averaged_not_summed():
    # Two cleaning invoices in the same period — each already the cost of one
    # turnover; the floor needs one representative value, not their sum
    # (ADR-0009 D3). recurrence=one_off replaces the old cost_type='one_time'.
    lines = [
        _line(
            "e1",
            60.0,
            "2026-06-01",
            "2026-06-30",
            recurrence="one_off",
            allocation_method="direct",
        ),
        _line(
            "e2",
            80.0,
            "2026-06-01",
            "2026-06-30",
            recurrence="one_off",
            allocation_method="direct",
        ),
    ]
    result = aggregate_cost(lines)
    assert result.per_booking_cost_eur == 70.0  # average, not 140.0


def test_no_one_off_lines_yields_zero():
    lines = [_line("e1", 100.0, "2026-06-01", "2026-06-30")]
    result = aggregate_cost(lines)
    assert result.per_booking_cost_eur == 0.0


def test_occupied_night_and_booking_methods_deferred_not_in_fixed_variable():
    # Phase 19: these two allocation methods need Phase 18 data
    # (occupied_nights/booking_count) not available in Stage A yet — they
    # must NOT be folded into fixed_cost_eur/variable_cost_eur here, and
    # must surface as a pending correction instead.
    lines = [
        _line(
            "e1",
            300.0,
            "2026-06-01",
            "2026-06-30",
            behavior="variable",
            concept="cleaning",
            allocation_method="occupied_night",
        ),
        _line(
            "e2",
            150.0,
            "2026-06-01",
            "2026-06-30",
            behavior="fixed",
            concept="other",
            allocation_method="booking",
        ),
        _line("e3", 60.0, "2026-06-01", "2026-06-30", behavior="variable"),
    ]
    result = aggregate_cost(lines)
    assert result.variable_cost_eur == 2.0  # only e3: 60/30
    assert result.fixed_cost_eur == 0.0
    corrections = {c.concept: c for c in result.pending_allocation_corrections}
    assert corrections["cleaning"].monthly_equivalent_eur == 300.0
    assert corrections["cleaning"].behavior == "variable"
    assert corrections["cleaning"].allocation_method == "occupied_night"
    assert corrections["other"].monthly_equivalent_eur == 150.0
    assert corrections["other"].allocation_method == "booking"


def test_no_deferred_lines_yields_empty_pending_corrections():
    lines = [_line("e1", 100.0, "2026-06-01", "2026-06-30")]
    result = aggregate_cost(lines)
    assert result.pending_allocation_corrections == ()


def test_ota_related_and_cleaning_sub_totals_are_split_from_concept():
    # Phase 11 (ADR-0011 backlog #5, spec 11 §4): the minimal slice of
    # backlog #3 this phase needs — ota_fee/channel_manager -> ota_related,
    # cleaning -> cleaning, both still fully counted in fixed/variable above.
    lines = [
        _line(
            "e1",
            300.0,
            "2026-06-01",
            "2026-06-30",
            behavior="variable",
            concept="ota_fee",
        ),
        _line(
            "e2",
            150.0,
            "2026-06-01",
            "2026-06-30",
            behavior="fixed",
            concept="channel_manager",
        ),
        _line(
            "e3",
            120.0,
            "2026-06-01",
            "2026-06-30",
            behavior="variable",
            concept="cleaning",
        ),
        _line(
            "e4",
            60.0,
            "2026-06-01",
            "2026-06-30",
            behavior="variable",
            concept="electricity",
        ),
    ]
    result = aggregate_cost(lines)
    assert result.ota_related_cost_eur == 15.0  # (300 + 150) / 30 days
    assert result.cleaning_cost_eur == 4.0  # 120 / 30 days
    # Still fully counted in the existing behavior totals — not removed.
    assert result.fixed_cost_eur == 5.0  # 150 / 30
    assert result.variable_cost_eur == 16.0  # (300 + 120 + 60) / 30


def test_no_ota_or_cleaning_lines_yields_zero_sub_totals():
    lines = [_line("e1", 100.0, "2026-06-01", "2026-06-30")]
    result = aggregate_cost(lines)
    assert result.ota_related_cost_eur == 0.0
    assert result.cleaning_cost_eur == 0.0
    assert result.laundry_cost_eur == 0.0
    assert result.booking_scope_cost_eur == 0.0


def test_laundry_and_booking_scope_sub_totals():
    # Phase 24 (ADR-0017 §3, spec 24 §5 AC-01/AC-02): laundry_cost_eur
    # mirrors cleaning_cost_eur; booking_scope_cost_eur sums every
    # scope='booking' line regardless of concept (here: ota_fee, cleaning,
    # laundry — electricity is scope='property', excluded).
    lines = [
        _line(
            "e1",
            300.0,
            "2026-06-01",
            "2026-06-30",
            behavior="variable",
            concept="ota_fee",
            scope="booking",
        ),
        _line(
            "e2",
            120.0,
            "2026-06-01",
            "2026-06-30",
            behavior="variable",
            concept="cleaning",
            scope="booking",
        ),
        _line(
            "e3",
            30.0,
            "2026-06-01",
            "2026-06-30",
            behavior="variable",
            concept="laundry",
            scope="booking",
        ),
        _line(
            "e4",
            60.0,
            "2026-06-01",
            "2026-06-30",
            behavior="variable",
            concept="electricity",
            scope="property",
        ),
    ]
    result = aggregate_cost(lines)
    assert result.laundry_cost_eur == 1.0  # 30 / 30 days
    assert result.booking_scope_cost_eur == 15.0  # (300 + 120 + 30) / 30 days


def test_cost_breakdown_groups_by_concept_per_day():
    # Phase 17 (ADR-0011 backlog #3): every concept observed, not just the 2
    # (ota_fee/channel_manager, cleaning) Phase 11 already split out.
    lines = [
        _line(
            "e1",
            300.0,
            "2026-06-01",
            "2026-06-30",
            behavior="variable",
            concept="electricity",
        ),
        _line(
            "e2",
            60.0,
            "2026-06-01",
            "2026-06-30",
            behavior="variable",
            concept="electricity",
        ),
        _line(
            "e3",
            120.0,
            "2026-06-01",
            "2026-06-30",
            behavior="fixed",
            concept="water",
        ),
    ]
    result = aggregate_cost(lines)
    assert result.cost_breakdown == (
        _concept_amount("electricity", 12.0),  # (300+60)/30
        _concept_amount("water", 4.0, behavior="fixed"),  # 120/30
    )


def test_cost_breakdown_follows_canonical_concept_order_not_input_order():
    lines = [
        _line("e1", 30.0, "2026-06-01", "2026-06-30", concept="other"),
        _line("e2", 30.0, "2026-06-01", "2026-06-30", concept="electricity"),
    ]
    result = aggregate_cost(lines)
    assert [entry.concept for entry in result.cost_breakdown] == [
        "electricity",
        "other",
    ]


def test_cost_breakdown_omits_concepts_with_no_lines():
    lines = [_line("e1", 100.0, "2026-06-01", "2026-06-30")]
    result = aggregate_cost(lines)
    assert result.cost_breakdown == (
        _concept_amount("electricity", round(100 / 30, 2)),
    )


def test_percentage_lines_excluded_from_fixed_variable():
    # Phase 20 (ADR-0013 §3): a percentage CostDefinition (e.g. ota_fee)
    # never contributes an EUR amount to fixed/variable — only its rate,
    # via percentage_costs. It still appears in cost_breakdown (display).
    lines = [
        _line(
            "e1",
            300.0,
            "2026-06-01",
            "2026-06-30",
            concept="ota_fee",
            calculation_base="pct_adjusted_revenue",
            revenue_base="total_revenue",
            rate=0.15,
            recurrence="per_booking",
            allocation_method="booking",
        ),
        _line("e2", 60.0, "2026-06-01", "2026-06-30", behavior="variable"),
    ]
    result = aggregate_cost(lines)
    assert result.variable_cost_eur == 2.0  # only e2: 60/30
    assert result.fixed_cost_eur == 0.0
    assert result.percentage_costs == (
        PercentageCostComponent(
            concept="ota_fee", rate=0.15, revenue_base="total_revenue"
        ),
    )
    assert {c.concept for c in result.cost_breakdown} == {"electricity", "ota_fee"}


def test_retained_billing_period_ends_keeps_top_two():
    ends = [date(2026, 6, 30), date(2026, 5, 31), date(2026, 4, 30)]
    assert retained_billing_period_ends(ends) == {date(2026, 6, 30), date(2026, 5, 31)}


@pytest.mark.parametrize("keep", [1, 3])
def test_retained_billing_period_ends_respects_keep(keep):
    ends = [date(2026, 6, 30), date(2026, 5, 31), date(2026, 4, 30)]
    assert len(retained_billing_period_ends(ends, keep=keep)) == min(keep, len(ends))
