from datetime import UTC, date, datetime

from fakes import FakeReadOnlyContext, FakeRuntimeContext
from flink_jobs.models import BookingRow, CostAggregate
from flink_jobs.stage_booking_enrichment import BookingEnrichmentFunction


def _cost(
    apartment_id="BCN-001",
    billing_period_start=date(2026, 6, 1),
    billing_period_end=date(2026, 6, 30),
):
    return CostAggregate(
        apartment_id=apartment_id,
        apartment_reference=apartment_id,
        city="Barcelona",
        neighborhood="Eixample",
        property_type="studio",
        bedrooms=0,
        fixed_cost_eur=0.0,
        variable_cost_eur=100.0,
        per_booking_cost_eur=0.0,
        total_monthly_cost_eur=3000.0,
        available_days=30,
        cost_lines_count=1,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        target_margin=0.05,
        competitiveness_discount=0.05,
        commission_pct=0.15,
        updated_at=datetime.now(UTC),
    )


def _booking(booking_id, check_in, check_out, status="confirmed"):
    return BookingRow(
        booking_id=booking_id,
        apartment_id="BCN-001",
        check_in=check_in,
        check_out=check_out,
        channel="airbnb",
        guests=2,
        revenue_eur=300.0,
        status=status,
    )


def _make_function():
    fn = BookingEnrichmentFunction()
    fn.open(FakeRuntimeContext())
    return fn


def test_cost_side_first_no_bookings_yet_emits_zero_occupied_nights():
    fn = _make_function()
    ctx = FakeReadOnlyContext(broadcast_state=None)
    results = list(fn.process_element1(_cost(), ctx))
    assert len(results) == 1
    assert results[0].occupied_nights == 0


def test_booking_arriving_before_any_cost_aggregate_does_not_emit():
    fn = _make_function()
    ctx = FakeReadOnlyContext(broadcast_state=None)
    results = list(
        fn.process_element2(
            _booking("b1", date(2026, 6, 10), date(2026, 6, 13)), ctx
        )
    )
    assert results == []


def test_cost_side_then_booking_side_recomputes_occupied_nights():
    fn = _make_function()
    ctx = FakeReadOnlyContext(broadcast_state=None)
    list(fn.process_element1(_cost(), ctx))
    results = list(
        fn.process_element2(
            _booking("b1", date(2026, 6, 10), date(2026, 6, 13)), ctx
        )
    )
    assert len(results) == 1
    assert results[0].occupied_nights == 3
    # Phase 20 (ADR-0013 §2): _booking() always seeds guests=2.
    assert results[0].avg_guests == 2.0


def test_booking_side_then_cost_side_recomputes_occupied_nights():
    """Same booking already known before the first CostAggregate for this
    apartment arrives — process_element1 must fold in whatever bookings
    already sit in state, not just bookings arriving afterward."""
    fn = _make_function()
    ctx = FakeReadOnlyContext(broadcast_state=None)
    list(fn.process_element2(_booking("b1", date(2026, 6, 10), date(2026, 6, 13)), ctx))
    results = list(fn.process_element1(_cost(), ctx))
    assert len(results) == 1
    assert results[0].occupied_nights == 3


def test_cancelled_booking_excluded_from_occupied_nights():
    fn = _make_function()
    ctx = FakeReadOnlyContext(broadcast_state=None)
    list(fn.process_element1(_cost(), ctx))
    results = list(
        fn.process_element2(
            _booking("b1", date(2026, 6, 10), date(2026, 6, 13), status="cancelled"),
            ctx,
        )
    )
    assert results[0].occupied_nights == 0


def test_upsert_by_booking_id_does_not_double_count():
    fn = _make_function()
    ctx = FakeReadOnlyContext(broadcast_state=None)
    list(fn.process_element1(_cost(), ctx))
    list(fn.process_element2(_booking("b1", date(2026, 6, 10), date(2026, 6, 13)), ctx))
    # Same booking_id, updated dates (e.g. a modification) — replaces, not adds.
    results = list(
        fn.process_element2(_booking("b1", date(2026, 6, 20), date(2026, 6, 22)), ctx)
    )
    assert results[0].occupied_nights == 2


def test_stale_booking_pruned_past_retention_buffer():
    fn = _make_function()
    ctx = FakeReadOnlyContext(broadcast_state=None)
    list(fn.process_element1(_cost(), ctx))
    # Well over 90 days before billing_period_start (2026-06-01).
    list(
        fn.process_element2(
            _booking("old", date(2025, 1, 1), date(2025, 1, 3)), ctx
        )
    )
    assert len(dict(fn.bookings.items())) == 0
