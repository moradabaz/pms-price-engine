from datetime import UTC, date, datetime, timedelta

from fakes import FakeReadOnlyContext, FakeRuntimeContext
from flink_jobs.decision_components import DecisionComponent
from flink_jobs.models import CostAggregate
from flink_jobs.stage_price_decision import DATA_STALE_TAG, PriceDecisionFunction
from shared_schemas.market_price import (
    MarketArea,
    MarketContext,
    MarketPrice,
    Pricing,
    PropertyProfile,
)


def _cost(
    apartment_id="BCN-001",
    variable_cost=100.0,
    updated_at=None,
    property_decision_components=(),
):
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
        property_decision_components=property_decision_components,
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


def test_los_floor_matrix_stay_length_1_matches_top_level_calculation():
    # AC-05: the stay_length=1 entry must match the top-level calculation
    # exactly, since the top-level calculation *is* the stay_length=1 case.
    fn, ctx = _make_function()
    list(fn.process_element1(_cost("apt-A", variable_cost=100.0), ctx))
    results = list(fn.process_element2(_market(days_from_today=7), ctx))

    calc = results[0].calculation
    matrix_by_stay_length = {c.stay_length: c for c in calc.los_floor_matrix}
    assert set(matrix_by_stay_length) == {1, 2, 3, 7, 14}

    los_1 = matrix_by_stay_length[1]
    assert los_1.minimum_price_eur == calc.minimum_price_eur
    assert los_1.floor_type == calc.floor_type
    assert los_1.floor_policy == calc.floor_policy
    assert los_1.rule_applied == calc.rule_applied
    assert los_1.suggested_price_eur == results[0].output.suggested_price_eur
    assert los_1.effective_margin == results[0].output.effective_margin


def test_decision_components_property_block_on_top_level_only():
    # Phase 10 (ADR-0011 backlog #4): calculation.decision_components carries
    # the property block + rule component; every los_floor_matrix candidate
    # carries only its own rule component, never the property block.
    property_components = (
        DecisionComponent(code="property_quality_tier", label="x", impact=0.30),
        DecisionComponent(code="property_rating", label="x", impact=0.08),
        DecisionComponent(code="property_view", label="x", impact=0.05),
        DecisionComponent(code="property_parking", label="x", impact=0.04),
    )
    fn, ctx = _make_function()
    list(
        fn.process_element1(
            _cost(
                "apt-A",
                variable_cost=100.0,
                property_decision_components=property_components,
            ),
            ctx,
        )
    )
    results = list(fn.process_element2(_market(days_from_today=7), ctx))

    calc = results[0].calculation
    assert len(calc.decision_components) == 5
    assert [c.code for c in calc.decision_components[:4]] == [
        c.code for c in property_components
    ]
    assert calc.decision_components[4].code.startswith("rule_")

    for candidate in calc.los_floor_matrix:
        assert len(candidate.decision_components) == 1
        assert candidate.decision_components[0].code == f"rule_{candidate.rule_applied}"


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
    # AC-02 (spec 12): contribution -> hard, structural_reduced_margin -> soft,
    # wired end-to-end through the real PriceDecisionFunction, not just pricing.py.
    assert by_date[str(near_night)].calculation.floor_policy == "hard"
    assert by_date[str(far_night)].calculation.rule_applied == "market_competitive"
    assert by_date[str(far_night)].calculation.floor_type == "structural_reduced_margin"
    assert by_date[str(far_night)].calculation.floor_policy == "soft"


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
