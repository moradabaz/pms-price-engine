from datetime import date

from fakes import FakeMultiState, FakeRuntimeContext
from flink_jobs.cost_aggregation import EnrichedPaymentLine
from flink_jobs.models import ApartmentSegmentRow, PricingStrategyRow
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


def _segment(apartment_id="BCN-001", **kwargs):
    defaults = dict(
        city="Barcelona", neighborhood="Eixample", property_type="studio", bedrooms=0
    )
    defaults.update(kwargs)
    return ApartmentSegmentRow(apartment_id=apartment_id, **defaults)


def _strategy(apartment_id="BCN-001", version=1, target_margin=0.05, **kwargs):
    return PricingStrategyRow(
        apartment_id=apartment_id,
        version=version,
        target_margin=target_margin,
        competitiveness_discount=kwargs.pop("competitiveness_discount", 0.05),
        **kwargs,
    )


def _make_function():
    fn = CostEnrichmentFunction()
    fn.open(FakeRuntimeContext())
    ctx = FakeMultiState()
    return fn, ctx


def test_no_emission_before_segment_or_strategy_arrives():
    fn, ctx = _make_function()
    results = list(fn.process_element(_line("e1", 100.0), ctx))
    assert results == []


def test_no_emission_with_only_segment_and_no_strategy():
    # Phase 25 (ADR-0018 §2): the two are now independent CDC streams —
    # neither alone is enough.
    fn, ctx = _make_function()
    fn.process_broadcast_element(_segment(), ctx)
    results = list(fn.process_element(_line("e1", 100.0), ctx))
    assert results == []


def test_no_emission_with_only_strategy_and_no_segment():
    fn, ctx = _make_function()
    fn.process_broadcast_element(_strategy(), ctx)
    results = list(fn.process_element(_line("e1", 100.0), ctx))
    assert results == []


def test_emits_cost_aggregate_after_segment_and_strategy_arrive():
    fn, ctx = _make_function()
    fn.process_broadcast_element(_segment(), ctx)
    fn.process_broadcast_element(_strategy(), ctx)
    results = list(fn.process_element(_line("e1", 100.0), ctx))

    assert len(results) == 1
    aggregate = results[0]
    assert aggregate.apartment_id == "BCN-001"
    assert aggregate.city == "Barcelona"
    assert aggregate.variable_cost_eur == round(100.0 / 30, 2)
    assert aggregate.fixed_cost_eur == 0.0
    assert aggregate.target_margin == 0.05
    assert aggregate.pricing_strategy_version == 1
    # Phase 11 (ADR-0011 backlog #5): Stage A no longer resolves
    # commission_pct at all — PricingStrategy doesn't carry it any more
    # (Phase 21, ADR-0014: split from the old SegmentAssignment).
    # This CostAggregate hasn't reached Stage A2 yet, so it still carries
    # CostAggregate's own pre-Stage-A2 default (spec 11 §3), not a resolved
    # value.
    assert aggregate.commission_pct == 0.15
    assert aggregate.commission_base == "total_revenue"
    # Default attributes (standard/4.0/no view/no parking) -> neutral factor.
    assert aggregate.property_attribute_factor == 1.0


def test_higher_pricing_strategy_version_replaces_lower():
    # Phase 25 (ADR-0018 §4, spec 25 §4 AC-03): version comparison, not
    # arrival order.
    fn, ctx = _make_function()
    fn.process_broadcast_element(_segment(), ctx)
    fn.process_broadcast_element(_strategy(version=1, target_margin=0.05), ctx)
    fn.process_broadcast_element(_strategy(version=2, target_margin=0.10), ctx)
    results = list(fn.process_element(_line("e1", 100.0), ctx))

    assert results[0].target_margin == 0.10
    assert results[0].pricing_strategy_version == 2


def test_out_of_order_lower_pricing_strategy_version_does_not_regress():
    fn, ctx = _make_function()
    fn.process_broadcast_element(_segment(), ctx)
    fn.process_broadcast_element(_strategy(version=2, target_margin=0.10), ctx)
    # A stale, out-of-order v1 arrives after v2 — must not regress.
    fn.process_broadcast_element(_strategy(version=1, target_margin=0.05), ctx)
    results = list(fn.process_element(_line("e1", 100.0), ctx))

    assert results[0].target_margin == 0.10
    assert results[0].pricing_strategy_version == 2


def test_property_attribute_factor_resolved_from_broadcast_attributes():
    fn, ctx = _make_function()
    fn.process_broadcast_element(
        _segment(quality_tier="luxury", rating=4.8, has_view=True, has_parking=True),
        ctx,
    )
    fn.process_broadcast_element(_strategy(), ctx)
    results = list(fn.process_element(_line("e1", 100.0), ctx))

    # 0.30 (luxury) + 0.08 (rating) + 0.05 (view) + 0.04 (parking) = 1.47
    assert results[0].property_attribute_factor == 1.47


def test_property_decision_components_resolved_from_broadcast_attributes():
    # Phase 10 (ADR-0011 backlog #4): Stage A resolves the component
    # breakdown alongside the plain factor, from the same broadcast state.
    fn, ctx = _make_function()
    fn.process_broadcast_element(
        _segment(quality_tier="luxury", rating=4.8, has_view=True, has_parking=True),
        ctx,
    )
    fn.process_broadcast_element(_strategy(), ctx)
    results = list(fn.process_element(_line("e1", 100.0), ctx))

    expected = property_attribute_components(
        quality_tier="luxury", rating=4.8, has_view=True, has_parking=True
    )
    assert list(results[0].property_decision_components) == expected


def test_ota_related_and_cleaning_sub_totals_pass_through_from_aggregation():
    # Phase 11 (ADR-0011 backlog #5): Stage A passes cost_aggregation.py's
    # new sub-totals onto CostAggregate unchanged.
    fn, ctx = _make_function()
    fn.process_broadcast_element(_segment(), ctx)
    fn.process_broadcast_element(_strategy(), ctx)
    line = _line("e1", 300.0, concept="ota_fee")
    results = list(fn.process_element(line, ctx))

    assert results[0].ota_related_cost_eur == round(300.0 / 30, 2)
    assert results[0].cleaning_cost_eur == 0.0


def test_upsert_by_event_id_does_not_double_count():
    fn, ctx = _make_function()
    fn.process_broadcast_element(_segment(), ctx)
    fn.process_broadcast_element(_strategy(), ctx)

    event_id = "e1"
    list(fn.process_element(_line(event_id, 100.0), ctx))
    results = list(fn.process_element(_line(event_id, 150.0), ctx))

    assert results[0].total_monthly_cost_eur == 150.0
