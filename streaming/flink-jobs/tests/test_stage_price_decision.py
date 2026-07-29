from datetime import UTC, date, datetime

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


def _cost(apartment_id="BCN-001", daily_cost=100.0, updated_at=None):
    return CostAggregate(
        apartment_id=apartment_id,
        apartment_reference=apartment_id,
        city="Barcelona",
        neighborhood="Eixample",
        property_type="studio",
        bedrooms=0,
        daily_cost_eur=daily_cost,
        total_monthly_cost_eur=daily_cost * 30,
        available_days=30,
        cost_lines_count=1,
        billing_period_start=date(2026, 6, 1),
        billing_period_end=date(2026, 6, 30),
        target_margin=0.05,
        competitiveness_discount=0.05,
        updated_at=updated_at or datetime.now(UTC),
    )


def _market(target_date="2026-08-10", avg_rate=90.0):
    return MarketPrice(
        event_id="00000000-0000-0000-0000-000000000001",
        market_area=MarketArea(
            city="Barcelona", neighborhood="Eixample", country_code="ES"
        ),
        property_profile=PropertyProfile(type="studio", bedrooms=0),
        target_date=target_date,
        pricing=Pricing(avg_nightly_rate=avg_rate),
        market_context=MarketContext(sample_size=20, data_source="mock"),
        collected_at=datetime.now(UTC),
    )


def _make_function():
    fn = PriceDecisionFunction()
    fn.open(FakeRuntimeContext())
    return fn, FakeReadOnlyContext(None)


def test_market_update_fans_out_across_known_apartments():
    fn, ctx = _make_function()
    list(fn.process_element1(_cost("apt-A", daily_cost=100.0), ctx))
    list(fn.process_element1(_cost("apt-B", daily_cost=140.0), ctx))

    results = list(fn.process_element2(_market(), ctx))

    assert {d.apartment_id for d in results} == {"apt-A", "apt-B"}
    assert all(d.calculation.rule_applied == "cost_protected" for d in results)


def test_cost_update_fans_out_across_known_nights():
    fn, ctx = _make_function()
    list(fn.process_element2(_market("2026-08-10", avg_rate=90.0), ctx))
    list(fn.process_element2(_market("2026-08-20", avg_rate=150.0), ctx))

    results = list(fn.process_element1(_cost("apt-A", daily_cost=140.0), ctx))

    by_date = {str(d.target_date): d for d in results}
    assert by_date["2026-08-10"].calculation.rule_applied == "cost_protected"
    assert by_date["2026-08-20"].calculation.rule_applied == "minimum_floor"


def test_past_target_date_is_dropped():
    fn, ctx = _make_function()
    past_event = _market("2020-01-01", avg_rate=100.0)
    assert list(fn.process_element2(past_event, ctx)) == []


def test_stale_replay_is_discarded():
    fn, ctx = _make_function()
    newer = datetime(2026, 1, 2, tzinfo=UTC)
    older = datetime(2026, 1, 1, tzinfo=UTC)
    list(fn.process_element1(_cost("apt-A", daily_cost=100.0, updated_at=newer), ctx))

    list(fn.process_element2(_market(), ctx))
    replay_results = list(
        fn.process_element1(_cost("apt-A", daily_cost=999.0, updated_at=older), ctx)
    )

    assert replay_results == []
    assert fn.apartments.get("apt-A").daily_cost_eur == 100.0


def test_on_timer_emits_data_stale_for_expired_key():
    fn, ctx = _make_function()
    list(fn.process_element1(_cost("apt-A"), ctx))
    fired_at_ms = ctx.timer_service_.registered[0]

    results = list(fn.on_timer(fired_at_ms, ctx))

    assert results == [(DATA_STALE_TAG, ("apartment", "apt-A"))]
