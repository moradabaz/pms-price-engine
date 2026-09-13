import json
from datetime import UTC, date, datetime

from fakes import FakeMultiState
from flink_jobs.models import (
    CompanyCostOccurrenceRow,
    CostAggregate,
    CostAllocationRuleRow,
    CostDefinitionRow,
)
from flink_jobs.stage_company_cost_enrichment import CompanyCostEnrichmentFunction


def _cost(apartment_id="BCN-001"):
    return CostAggregate(
        apartment_id=apartment_id,
        apartment_reference=apartment_id,
        city="Barcelona",
        neighborhood="Eixample",
        property_type="studio",
        bedrooms=0,
        fixed_cost_eur=10.0,
        variable_cost_eur=5.0,
        per_booking_cost_eur=0.0,
        total_monthly_cost_eur=450.0,
        available_days=30,
        cost_lines_count=1,
        billing_period_start=date(2026, 6, 1),
        billing_period_end=date(2026, 6, 30),
        target_margin=0.05,
        competitiveness_discount=0.05,
        updated_at=datetime.now(UTC),
    )


def _seed_office_rent(ctx, weight_config):
    cost_definition_id = "00000000-0000-0000-0000-000000000001"
    fn_input = [
        CostDefinitionRow(
            cost_definition_id=cost_definition_id,
            concept="office_rent",
            scope="company",
            behavior="fixed",
            trigger="time",
            calculation_base="fixed_amount",
            recurrence="monthly",
            revenue_base=None,
        ),
        CostAllocationRuleRow(
            cost_definition_id=cost_definition_id,
            method="weighted",
            weight_config=json.dumps(weight_config),
        ),
        CompanyCostOccurrenceRow(
            company_cost_occurrence_id="00000000-0000-0000-0000-000000000002",
            cost_definition_id=cost_definition_id,
            billing_period_start=date(2026, 6, 1),
            billing_period_end=date(2026, 6, 30),
            amount_gross=1200.0,
        ),
    ]
    fn = CompanyCostEnrichmentFunction()
    for row in fn_input:
        fn.process_broadcast_element(row, ctx)
    return fn


def test_passthrough_when_no_occurrence_known_yet():
    fn = CompanyCostEnrichmentFunction()
    ctx = FakeMultiState()
    results = list(fn.process_element(_cost(), ctx))
    assert len(results) == 1
    assert results[0].fixed_cost_eur == 10.0


def test_equal_split_across_two_apartments():
    ctx = FakeMultiState()
    fn = _seed_office_rent(ctx, {"BCN-001": 0.5, "BCN-002": 0.5})

    results_a = list(fn.process_element(_cost("BCN-001"), ctx))
    results_b = list(fn.process_element(_cost("BCN-002"), ctx))

    # 1200 * 0.5 / 30 days = 20 EUR/night added to fixed_cost_eur
    assert results_a[0].fixed_cost_eur == 10.0 + 20.0
    assert results_b[0].fixed_cost_eur == 10.0 + 20.0


def test_apartment_absent_from_weight_map_gets_no_share():
    ctx = FakeMultiState()
    fn = _seed_office_rent(ctx, {"BCN-001": 1.0})

    results = list(fn.process_element(_cost("BCN-999"), ctx))
    assert results[0].fixed_cost_eur == 10.0


def test_variable_behavior_definition_adds_to_variable_bucket():
    cost_definition_id = "00000000-0000-0000-0000-000000000001"
    ctx = FakeMultiState()
    fn = CompanyCostEnrichmentFunction()
    fn.process_broadcast_element(
        CostDefinitionRow(
            cost_definition_id=cost_definition_id,
            concept="office_rent",
            scope="company",
            behavior="variable",
            trigger="time",
            calculation_base="fixed_amount",
            recurrence="monthly",
            revenue_base=None,
        ),
        ctx,
    )
    fn.process_broadcast_element(
        CostAllocationRuleRow(
            cost_definition_id=cost_definition_id,
            method="weighted",
            weight_config=json.dumps({"BCN-001": 1.0}),
        ),
        ctx,
    )
    fn.process_broadcast_element(
        CompanyCostOccurrenceRow(
            company_cost_occurrence_id="00000000-0000-0000-0000-000000000002",
            cost_definition_id=cost_definition_id,
            billing_period_start=date(2026, 6, 1),
            billing_period_end=date(2026, 6, 30),
            amount_gross=600.0,
        ),
        ctx,
    )

    results = list(fn.process_element(_cost("BCN-001"), ctx))
    assert results[0].variable_cost_eur == 5.0 + 20.0  # 600 / 30
    assert results[0].fixed_cost_eur == 10.0
