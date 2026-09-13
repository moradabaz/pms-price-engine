from datetime import date

from fakes import (
    FakeBroadcastContext,
    FakeMapState,
    FakeReadOnlyContext,
    FakeRuntimeContext,
)
from flink_jobs.cost_aggregation import EnrichedPaymentLine
from flink_jobs.models import ApartmentSegmentRow
from flink_jobs.stage_cost_enrichment import CostEnrichmentFunction
from pricing_formulas.layers.structural import property_attribute_components


def _line(
    event_id,
    amount,
    period_start="2026-06-01",
    period_end="2026-06-30",
    behavior="variable",
    concept="electricity",
):
    return EnrichedPaymentLine(
        event_id=event_id,
        apartment_id="BCN-001",
        apartment_reference="BCN-001",
        billing_period_start=date.fromisoformat(period_start),
        billing_period_end=date.fromisoformat(period_end),
        amount_gross=amount,
        concept=concept,
        scope="property",
        behavior=behavior,
        trigger="time",
        calculation_base="fixed_amount",
        recurrence="monthly",
        revenue_base=None,
        allocation_method="calendar_day",
        weight_config=None,
    )


def _make_function():
    fn = CostEnrichmentFunction()
    runtime_context = FakeRuntimeContext()
    fn.open(runtime_context)
    broadcast_state = FakeMapState()
    return fn, broadcast_state


def test_no_emission_before_segment_assignment_arrives():
    fn, broadcast_state = _make_function()
    ctx = FakeReadOnlyContext(broadcast_state)
    results = list(fn.process_element(_line("e1", 100.0), ctx))
    assert results == []


def test_emits_cost_aggregate_after_segment_arrives():
    fn, broadcast_state = _make_function()
    read_ctx = FakeReadOnlyContext(broadcast_state)
    broadcast_ctx = FakeBroadcastContext(broadcast_state)

    fn.process_broadcast_element(
        ApartmentSegmentRow(
            "BCN-001", "Barcelona", "Eixample", "studio", 0, 0.05, 0.05
        ),
        broadcast_ctx,
    )
    results = list(fn.process_element(_line("e1", 100.0), read_ctx))

    assert len(results) == 1
    aggregate = results[0]
    assert aggregate.apartment_id == "BCN-001"
    assert aggregate.city == "Barcelona"
    assert aggregate.variable_cost_eur == round(100.0 / 30, 2)
    assert aggregate.fixed_cost_eur == 0.0
    # Phase 11 (ADR-0011 backlog #5): Stage A no longer resolves
    # commission_pct at all — SegmentAssignment doesn't carry it any more.
    # This CostAggregate hasn't reached Stage A2 yet, so it still carries
    # CostAggregate's own pre-Stage-A2 default (spec 11 §3), not a resolved
    # value.
    assert aggregate.commission_pct == 0.15
    assert aggregate.commission_base == "total_revenue"
    # Default attributes (standard/4.0/no view/no parking) -> neutral factor.
    assert aggregate.property_attribute_factor == 1.0


def test_property_attribute_factor_resolved_from_broadcast_attributes():
    fn, broadcast_state = _make_function()
    read_ctx = FakeReadOnlyContext(broadcast_state)
    fn.process_broadcast_element(
        ApartmentSegmentRow(
            "BCN-001",
            "Barcelona",
            "Eixample",
            "studio",
            0,
            0.05,
            0.05,
            quality_tier="luxury",
            rating=4.8,
            has_view=True,
            has_parking=True,
        ),
        FakeBroadcastContext(broadcast_state),
    )
    results = list(fn.process_element(_line("e1", 100.0), read_ctx))

    # 0.30 (luxury) + 0.08 (rating) + 0.05 (view) + 0.04 (parking) = 1.47
    assert results[0].property_attribute_factor == 1.47


def test_property_decision_components_resolved_from_broadcast_attributes():
    # Phase 10 (ADR-0011 backlog #4): Stage A resolves the component
    # breakdown alongside the plain factor, from the same broadcast state.
    fn, broadcast_state = _make_function()
    read_ctx = FakeReadOnlyContext(broadcast_state)
    fn.process_broadcast_element(
        ApartmentSegmentRow(
            "BCN-001",
            "Barcelona",
            "Eixample",
            "studio",
            0,
            0.05,
            0.05,
            quality_tier="luxury",
            rating=4.8,
            has_view=True,
            has_parking=True,
        ),
        FakeBroadcastContext(broadcast_state),
    )
    results = list(fn.process_element(_line("e1", 100.0), read_ctx))

    expected = property_attribute_components(
        quality_tier="luxury", rating=4.8, has_view=True, has_parking=True
    )
    assert list(results[0].property_decision_components) == expected


def test_ota_related_and_cleaning_sub_totals_pass_through_from_aggregation():
    # Phase 11 (ADR-0011 backlog #5): Stage A passes cost_aggregation.py's
    # new sub-totals onto CostAggregate unchanged.
    fn, broadcast_state = _make_function()
    read_ctx = FakeReadOnlyContext(broadcast_state)
    fn.process_broadcast_element(
        ApartmentSegmentRow(
            "BCN-001", "Barcelona", "Eixample", "studio", 0, 0.05, 0.05
        ),
        FakeBroadcastContext(broadcast_state),
    )
    line = _line("e1", 300.0, concept="ota_fee")
    results = list(fn.process_element(line, read_ctx))

    assert results[0].ota_related_cost_eur == round(300.0 / 30, 2)
    assert results[0].cleaning_cost_eur == 0.0


def test_upsert_by_event_id_does_not_double_count():
    fn, broadcast_state = _make_function()
    read_ctx = FakeReadOnlyContext(broadcast_state)
    fn.process_broadcast_element(
        ApartmentSegmentRow(
            "BCN-001", "Barcelona", "Eixample", "studio", 0, 0.05, 0.05
        ),
        FakeBroadcastContext(broadcast_state),
    )

    event_id = "e1"
    list(fn.process_element(_line(event_id, 100.0), read_ctx))
    results = list(fn.process_element(_line(event_id, 150.0), read_ctx))

    assert results[0].total_monthly_cost_eur == 150.0
