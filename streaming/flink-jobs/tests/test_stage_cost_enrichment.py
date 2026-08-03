from fakes import (
    FakeBroadcastContext,
    FakeMapState,
    FakeReadOnlyContext,
    FakeRuntimeContext,
)
from flink_jobs.models import ApartmentSegmentRow
from flink_jobs.stage_cost_enrichment import CostEnrichmentFunction
from shared_schemas.payment_line import PaymentLine


def _line(event_id, amount, period_start="2026-06-01", period_end="2026-06-30"):
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


def _make_function():
    fn = CostEnrichmentFunction()
    runtime_context = FakeRuntimeContext()
    fn.open(runtime_context)
    broadcast_state = FakeMapState()
    return fn, broadcast_state


def test_no_emission_before_segment_assignment_arrives():
    fn, broadcast_state = _make_function()
    ctx = FakeReadOnlyContext(broadcast_state)
    results = list(
        fn.process_element(_line("00000000-0000-0000-0000-000000000001", 100.0), ctx)
    )
    assert results == []


def test_emits_cost_aggregate_after_segment_arrives():
    fn, broadcast_state = _make_function()
    read_ctx = FakeReadOnlyContext(broadcast_state)
    broadcast_ctx = FakeBroadcastContext(broadcast_state)

    fn.process_broadcast_element(
        ApartmentSegmentRow(
            "BCN-001", "Barcelona", "Eixample", "studio", 0, 0.05, 0.05, 0.15
        ),
        broadcast_ctx,
    )
    results = list(
        fn.process_element(
            _line("00000000-0000-0000-0000-000000000001", 100.0), read_ctx
        )
    )

    assert len(results) == 1
    aggregate = results[0]
    assert aggregate.apartment_id == "BCN-001"
    assert aggregate.city == "Barcelona"
    assert aggregate.variable_cost_eur == round(100.0 / 30, 2)
    assert aggregate.fixed_cost_eur == 0.0
    assert aggregate.commission_pct == 0.15


def test_upsert_by_event_id_does_not_double_count():
    fn, broadcast_state = _make_function()
    read_ctx = FakeReadOnlyContext(broadcast_state)
    fn.process_broadcast_element(
        ApartmentSegmentRow(
            "BCN-001", "Barcelona", "Eixample", "studio", 0, 0.05, 0.05, 0.15
        ),
        FakeBroadcastContext(broadcast_state),
    )

    event_id = "00000000-0000-0000-0000-000000000001"
    list(fn.process_element(_line(event_id, 100.0), read_ctx))
    results = list(fn.process_element(_line(event_id, 150.0), read_ctx))

    assert results[0].total_monthly_cost_eur == 150.0
