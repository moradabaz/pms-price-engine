from datetime import UTC, date, datetime, timedelta

from fakes import FakeReadOnlyContext, FakeRuntimeContext
from flink_jobs.models import CostAggregate
from flink_jobs.stage_price_decision import DATA_STALE_TAG, PriceDecisionFunction
from shared_schemas.market_price import (
    MarketArea,
    MarketContext,
    MarketPrice,
    Pricing,
    PropertyProfile,
)


def _cost(apartment_id="BCN-001", variable_cost=100.0, updated_at=None):
    return CostAggregate(
        apartment_id=apartment_id,
        apartment_reference=apartment_id,
        city="Barcelona",
        neighborhood="Eixample",
        property_type="studio",
        bedrooms=0,
        fixed_cost_eur=0.0,
        variable_cost_eur=variable_cost,
        one_time_cost_eur=0.0,
        total_monthly_cost_eur=variable_cost * 30,
        available_days=30,
        cost_lines_count=1,
        billing_period_start=date(2026, 6, 1),
        billing_period_end=date(2026, 6, 30),
        target_margin=0.05,
        competitiveness_discount=0.05,
        commission_pct=0.15,
        updated_at=updated_at or datetime.now(UTC),
    )


def _market(days_from_today=7, avg_rate=90.0, collected_at=None, target_date=None):
    """target_date defaults to `days_from_today` days from the real today, so
    tier-dependent assertions (ADR-0009) stay correct regardless of when the
    suite actually runs. Pass target_date directly for a fixed past date."""
    resolved_date = target_date or (date.today() + timedelta(days=days_from_today))
    return MarketPrice(
        event_id="00000000-0000-0000-0000-000000000001",
        market_area=MarketArea(
            city="Barcelona", neighborhood="Eixample", country_code="ES"
        ),
        property_profile=PropertyProfile(type="studio", bedrooms=0),
        target_date=str(resolved_date),
        pricing=Pricing(avg_nightly_rate=avg_rate),
        market_context=MarketContext(sample_size=20, data_source="mock"),
        collected_at=collected_at or datetime.now(UTC),
    )


def _make_function():
    fn = PriceDecisionFunction()
    fn.open(FakeRuntimeContext())
    return fn, FakeReadOnlyContext(None)


def test_market_update_fans_out_across_known_apartments():
    fn, ctx = _make_function()
    list(fn.process_element1(_cost("apt-A", variable_cost=100.0), ctx))
    list(fn.process_element1(_cost("apt-B", variable_cost=140.0), ctx))

    # 7 days out -> contribution floor; both comfortably above avg_rate=90.
    results = list(fn.process_element2(_market(days_from_today=7), ctx))

    assert {d.apartment_id for d in results} == {"apt-A", "apt-B"}
    assert all(d.calculation.rule_applied == "cost_protected" for d in results)


def test_cost_update_fans_out_across_known_nights():
    fn, ctx = _make_function()
    near_night = date.today() + timedelta(days=7)  # contribution floor
    far_night = date.today() + timedelta(days=20)  # structural_reduced_margin floor
    list(fn.process_element2(_market(target_date=near_night, avg_rate=90.0), ctx))
    list(fn.process_element2(_market(target_date=far_night, avg_rate=300.0), ctx))

    results = list(fn.process_element1(_cost("apt-A", variable_cost=140.0), ctx))

    by_date = {str(d.target_date): d for d in results}
    assert by_date[str(near_night)].calculation.rule_applied == "cost_protected"
    assert by_date[str(near_night)].calculation.floor_type == "contribution"
    assert by_date[str(far_night)].calculation.rule_applied == "market_competitive"
    assert by_date[str(far_night)].calculation.floor_type == "structural_reduced_margin"


def test_past_target_date_is_dropped():
    fn, ctx = _make_function()
    past_event = _market(target_date=date(2020, 1, 1), avg_rate=100.0)
    assert list(fn.process_element2(past_event, ctx)) == []


def test_stale_replay_is_discarded():
    fn, ctx = _make_function()
    newer = datetime(2026, 1, 2, tzinfo=UTC)
    older = datetime(2026, 1, 1, tzinfo=UTC)
    list(
        fn.process_element1(_cost("apt-A", variable_cost=100.0, updated_at=newer), ctx)
    )

    list(fn.process_element2(_market(), ctx))
    replay_results = list(
        fn.process_element1(_cost("apt-A", variable_cost=999.0, updated_at=older), ctx)
    )

    assert replay_results == []
    assert fn.apartments.get("apt-A").variable_cost_eur == 100.0


def test_on_timer_emits_data_stale_for_expired_key():
    fn, ctx = _make_function()
    list(fn.process_element1(_cost("apt-A"), ctx))
    fired_at_ms = ctx.timer_service_.registered[0]

    results = list(fn.on_timer(fired_at_ms, ctx))

    assert results == [(DATA_STALE_TAG, ("apartment", "apt-A"))]


def test_data_age_seconds_clamps_to_zero_under_clock_skew():
    fn, ctx = _make_function()
    list(fn.process_element1(_cost("apt-A"), ctx))
    future_collected_at = datetime.now(UTC).replace(year=2099)

    results = list(fn.process_element2(_market(collected_at=future_collected_at), ctx))

    assert results[0].market_inputs.data_age_seconds == 0


def test_on_timer_emits_data_stale_for_expired_night():
    fn, ctx = _make_function()
    list(fn.process_element2(_market(), ctx))
    fired_at_ms = ctx.timer_service_.registered[0]

    results = list(fn.on_timer(fired_at_ms, ctx))

    night_key = str(date.today() + timedelta(days=7))
    assert results == [(DATA_STALE_TAG, ("night", night_key))]
