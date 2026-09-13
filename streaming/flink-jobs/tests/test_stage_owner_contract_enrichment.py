from datetime import UTC, date, datetime

from fakes import FakeMultiState
from flink_jobs.models import CostAggregate, CostDefinitionRow, OwnerContractRow
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
        per_booking_cost_eur=0.0,
        total_monthly_cost_eur=3000.0,
        available_days=30,
        cost_lines_count=1,
        billing_period_start=date(2026, 6, 1),
        billing_period_end=date(2026, 6, 30),
        target_margin=0.05,
        competitiveness_discount=0.05,
        updated_at=datetime.now(UTC),
    )


def _seed(ctx, cost_definition_id="00000000-0000-0000-0000-000000000001", rate=0.18,
          revenue_base="revenue_minus_ota"):
    fn = OwnerContractEnrichmentFunction()
    fn.process_broadcast_element(
        OwnerContractRow("BCN-001", "OWN-001", cost_definition_id), ctx
    )
    fn.process_broadcast_element(
        CostDefinitionRow(
            cost_definition_id=cost_definition_id,
            concept="owner_commission",
            scope="booking",
            behavior="variable",
            trigger="revenue",
            calculation_base="pct_adjusted_revenue",
            recurrence="per_booking",
            revenue_base=revenue_base,
            rate=rate,
        ),
        ctx,
    )
    return fn


def test_no_emission_before_owner_contract_arrives():
    fn = OwnerContractEnrichmentFunction()
    ctx = FakeMultiState()
    results = list(fn.process_element(_cost(), ctx))
    assert results == []


def test_no_emission_when_contract_known_but_cost_definition_not_yet():
    ctx = FakeMultiState()
    fn = OwnerContractEnrichmentFunction()
    fn.process_broadcast_element(
        OwnerContractRow("BCN-001", "OWN-001", "00000000-0000-0000-0000-000000000001"),
        ctx,
    )
    results = list(fn.process_element(_cost(), ctx))
    assert results == []


def test_resolves_commission_base_and_pct_once_contract_arrives():
    ctx = FakeMultiState()
    fn = _seed(ctx)

    results = list(fn.process_element(_cost(), ctx))

    assert len(results) == 1
    assert results[0].commission_base == "revenue_minus_ota"
    assert results[0].commission_pct == 0.18


def test_preserves_every_other_cost_aggregate_field():
    ctx = FakeMultiState()
    fn = _seed(ctx, rate=0.15, revenue_base="total_revenue")
    cost = _cost()

    result = list(fn.process_element(cost, ctx))[0]

    assert result.variable_cost_eur == cost.variable_cost_eur
    assert result.city == cost.city
    assert result.target_margin == cost.target_margin
