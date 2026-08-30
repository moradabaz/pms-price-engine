from datetime import UTC, date, datetime

from fakes import FakeBroadcastContext, FakeMapState, FakeReadOnlyContext
from flink_jobs.models import CostAggregate, OwnerContractRow
from flink_jobs.stage_owner_contract_enrichment import OwnerContractEnrichmentFunction


def _cost(apartment_id="BCN-001"):
    return CostAggregate(
        apartment_id=apartment_id,
        apartment_reference=apartment_id,
        city="Barcelona",
        neighborhood="Eixample",
        property_type="studio",
        bedrooms=0,
        fixed_cost_eur=0.0,
        variable_cost_eur=100.0,
        one_time_cost_eur=0.0,
        total_monthly_cost_eur=3000.0,
        available_days=30,
        cost_lines_count=1,
        billing_period_start=date(2026, 6, 1),
        billing_period_end=date(2026, 6, 30),
        target_margin=0.05,
        competitiveness_discount=0.05,
        updated_at=datetime.now(UTC),
    )


def _make_function():
    fn = OwnerContractEnrichmentFunction()
    broadcast_state = FakeMapState()
    return fn, broadcast_state


def test_no_emission_before_owner_contract_arrives():
    fn, broadcast_state = _make_function()
    ctx = FakeReadOnlyContext(broadcast_state)
    results = list(fn.process_element(_cost(), ctx))
    assert results == []


def test_resolves_commission_base_and_pct_once_contract_arrives():
    fn, broadcast_state = _make_function()
    read_ctx = FakeReadOnlyContext(broadcast_state)
    fn.process_broadcast_element(
        OwnerContractRow("BCN-001", "OWN-001", "revenue_minus_ota", 0.18),
        FakeBroadcastContext(broadcast_state),
    )

    results = list(fn.process_element(_cost(), read_ctx))

    assert len(results) == 1
    assert results[0].commission_base == "revenue_minus_ota"
    assert results[0].commission_pct == 0.18


def test_preserves_every_other_cost_aggregate_field():
    fn, broadcast_state = _make_function()
    read_ctx = FakeReadOnlyContext(broadcast_state)
    fn.process_broadcast_element(
        OwnerContractRow("BCN-001", "OWN-001", "total_revenue", 0.15),
        FakeBroadcastContext(broadcast_state),
    )
    cost = _cost()

    result = list(fn.process_element(cost, read_ctx))[0]

    assert result.variable_cost_eur == cost.variable_cost_eur
    assert result.city == cost.city
    assert result.target_margin == cost.target_margin
