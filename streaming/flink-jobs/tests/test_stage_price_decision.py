from datetime import UTC, date, datetime, timedelta

from fakes import FakeReadOnlyContext, FakeRuntimeContext
from flink_jobs.models import CostAggregate
from flink_jobs.stage_price_decision import DATA_STALE_TAG, PriceDecisionFunction
from pricing_formulas.decision_components import DecisionComponent
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
    one_time_cost=0.0,
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
        one_time_cost_eur=one_time_cost,
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


def _market(
    days_from_today=7,
    avg_rate=90.0,
    collected_at=None,
    target_date=None,
    platform=None,
):
    """target_date defaults to `days_from_today` days from the real today, so
    tier-dependent assertions (ADR-0009) stay correct regardless of when the
    suite actually runs. Pass target_date directly for a fixed past date.
    platform=None (the default) is the existing blended event every test
    before Phase 16 already exercises; pass a real value for a channel event
    (ADR-0011 backlog #2)."""
    resolved_date = target_date or (date.today() + timedelta(days=days_from_today))
    return MarketPrice(
        event_id="00000000-0000-0000-0000-000000000001",
        market_area=MarketArea(
            city="Barcelona", neighborhood="Eixample", country_code="ES"
        ),
        property_profile=PropertyProfile(type="studio", bedrooms=0),
        target_date=str(resolved_date),
        pricing=Pricing(avg_nightly_rate=avg_rate),
        market_context=MarketContext(
            sample_size=20, data_source="mock", platform=platform
        ),
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
            # variable_cost=10.0 (not cost_protected, per _market's default
            # avg_rate=90.0 below) so Phase 15's minimum_stay_* component
            # never fires here — this test is only about property/rule
            # component ordering.
            _cost(
                "apt-A",
                variable_cost=10.0,
                property_decision_components=property_components,
            ),
            ctx,
        )
    )
    results = list(fn.process_element2(_market(days_from_today=7), ctx))

    calc = results[0].calculation
    assert calc.rule_applied != "cost_protected"
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


def test_minimum_stay_recommendation_present_and_fine_at_los_1():
    # AC-05 (spec 15): a decision whose stay_length=1 is not cost_protected
    # gets recommended_min_stay=1, floor_relief_eur=0.0, and no extra
    # decision component appended.
    fn, ctx = _make_function()
    list(fn.process_element1(_cost("apt-A", variable_cost=10.0), ctx))
    results = list(fn.process_element2(_market(days_from_today=7), ctx))

    calc = results[0].calculation
    assert calc.rule_applied != "cost_protected"
    recommendation = calc.minimum_stay_recommendation
    assert recommendation.recommended_min_stay == 1
    assert recommendation.floor_relief_eur == 0.0
    # AC (reservation totals): at LOS 1, the reservation total matches the
    # top-level, per-night cost/price exactly (a 1-night "reservation").
    assert recommendation.cost_per_reservation_eur == 10.0
    assert recommendation.suggested_price_per_reservation_eur == (
        results[0].output.suggested_price_eur
    )
    assert not any(
        c.code in ("minimum_stay_recommended", "minimum_stay_not_viable")
        for c in calc.decision_components
    )


def test_minimum_stay_recommendation_found_when_one_time_cost_dilutes():
    # AC-05 / spec 15 §4's worked example, wired through the real
    # PriceDecisionFunction: Cr=110.0 forces cost_protected at stay_length=1
    # but LOS 2 already dilutes it enough to clear the floor.
    fn, ctx = _make_function()
    list(
        fn.process_element1(
            _cost("apt-A", variable_cost=0.0, one_time_cost=110.0), ctx
        )
    )
    results = list(fn.process_element2(_market(days_from_today=45, avg_rate=90.0), ctx))

    calc = results[0].calculation
    assert calc.rule_applied == "cost_protected"
    recommendation = calc.minimum_stay_recommendation
    assert recommendation.recommended_min_stay == 2
    assert recommendation.floor_relief_eur == round(137.5 - 68.75, 2)
    # AC (reservation totals): a 2-night reservation pays the 110.0 one-time
    # cost once, priced at the LOS-2 candidate's own 85.5 EUR/night x 2.
    assert recommendation.cost_per_reservation_eur == 110.0
    assert recommendation.suggested_price_per_reservation_eur == round(85.5 * 2, 2)
    assert [c.code for c in calc.decision_components][-1] == "minimum_stay_recommended"


def test_minimum_stay_recommendation_not_viable_when_only_variable_cost_is_the_issue():
    # AC-05: one_time_cost_eur=0.0 here (the _cost() default), so
    # minimum_price_eur is identical across every LOS candidate (nothing to
    # dilute) — cost_protected at stay_length=1 stays cost_protected at
    # every candidate. recommended_min_stay is correctly None, and the
    # minimum_stay_not_viable component is appended after the existing ones.
    fn, ctx = _make_function()
    list(fn.process_element1(_cost("apt-A", variable_cost=140.0), ctx))
    results = list(fn.process_element2(_market(days_from_today=7, avg_rate=90.0), ctx))

    calc = results[0].calculation
    assert calc.rule_applied == "cost_protected"
    assert all(c.rule_applied == "cost_protected" for c in calc.los_floor_matrix)
    recommendation = calc.minimum_stay_recommendation
    assert recommendation.recommended_min_stay is None
    assert recommendation.floor_relief_eur is None
    assert recommendation.cost_per_reservation_eur is None
    assert recommendation.suggested_price_per_reservation_eur is None
    assert [c.code for c in calc.decision_components][-1] == "minimum_stay_not_viable"


def test_channel_price_matrix_empty_when_no_channel_event_has_arrived():
    # AC-01: only the blended event has ever arrived for this night.
    fn, ctx = _make_function()
    list(fn.process_element1(_cost("apt-A", variable_cost=10.0), ctx))
    results = list(fn.process_element2(_market(days_from_today=7), ctx))

    assert results[0].calculation.channel_price_matrix == []


def test_channel_event_populates_matrix_without_touching_top_level():
    # AC-02/AC-04: a channel event for an already-known night adds a
    # candidate to channel_price_matrix; the top-level calculation (still
    # keyed off the blended rate) is byte-for-byte identical to before.
    fn, ctx = _make_function()
    list(fn.process_element1(_cost("apt-A", variable_cost=10.0), ctx))
    before = list(fn.process_element2(_market(days_from_today=7), ctx))[0]

    results = list(
        fn.process_element2(
            _market(days_from_today=7, avg_rate=97.2, platform="airbnb"), ctx
        )
    )

    after = results[0]
    assert after.calculation.model_dump(
        exclude={"channel_price_matrix"}
    ) == before.calculation.model_dump(exclude={"channel_price_matrix"})
    assert len(after.calculation.channel_price_matrix) == 1
    candidate = after.calculation.channel_price_matrix[0]
    assert candidate.platform == "airbnb"
    assert candidate.avg_nightly_rate_eur == 97.2


def test_channel_only_event_for_unknown_night_does_not_fan_out():
    # A channel-only event for a night whose blended snapshot hasn't arrived
    # yet is stored but doesn't trigger a fan-out (spec 16 §2 "known night").
    fn, ctx = _make_function()
    list(fn.process_element1(_cost("apt-A", variable_cost=10.0), ctx))

    results = list(
        fn.process_element2(
            _market(days_from_today=7, avg_rate=97.2, platform="airbnb"), ctx
        )
    )

    assert results == []


def test_channel_snapshot_survives_a_later_blended_update():
    # A channel arriving before the blended event for a brand-new night is
    # included once the blended event finally arrives — nothing is lost.
    fn, ctx = _make_function()
    list(fn.process_element1(_cost("apt-A", variable_cost=10.0), ctx))
    list(
        fn.process_element2(
            _market(days_from_today=7, avg_rate=97.2, platform="airbnb"), ctx
        )
    )

    results = list(fn.process_element2(_market(days_from_today=7), ctx))

    assert len(results) == 1
    matrix = results[0].calculation.channel_price_matrix
    assert [c.platform for c in matrix] == ["airbnb"]


def test_multiple_channels_can_have_different_rule_applied():
    # AC-03: unlike LOS's single monotonic axis, channels are independent —
    # a high, uncompetitive rate for one channel and a very low rate for
    # another land on different rule_applied values within the same decision.
    fn, ctx = _make_function()
    list(fn.process_element1(_cost("apt-A", variable_cost=5.0, one_time_cost=0.0), ctx))
    list(fn.process_element2(_market(days_from_today=7, avg_rate=200.0), ctx))
    list(
        fn.process_element2(
            _market(days_from_today=7, avg_rate=200.0, platform="airbnb"), ctx
        )
    )
    results = list(
        fn.process_element2(
            _market(days_from_today=7, avg_rate=3.0, platform="booking"), ctx
        )
    )

    matrix = {c.platform: c for c in results[0].calculation.channel_price_matrix}
    assert matrix["airbnb"].rule_applied == "market_competitive"
    assert matrix["booking"].rule_applied == "cost_protected"


def test_on_timer_emits_data_stale_for_expired_night():
    fn, ctx = _make_function()
    list(fn.process_element2(_market(), ctx))
    fired_at_ms = ctx.timer_service_.registered[0]

    results = list(fn.on_timer(fired_at_ms, ctx))

    night_key = str(date.today() + timedelta(days=7))
    assert results == [(DATA_STALE_TAG, ("night", night_key))]
