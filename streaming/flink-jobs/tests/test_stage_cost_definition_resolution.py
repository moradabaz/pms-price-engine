from datetime import date

from fakes import FakeMultiState
from flink_jobs.models import CostAllocationRuleRow, CostDefinitionRow
from flink_jobs.stage_cost_definition_resolution import CostDefinitionResolutionFunction
from shared_schemas.payment_line import PaymentLine


def _payment_line(cost_definition_id):
    return PaymentLine.model_validate(
        {
            "event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
            "schema_version": "2.0",
            "apartment_id": "BCN-001",
            "apartment_reference": "BCN-001",
            "cost_definition_id": cost_definition_id,
            "description": "test",
            "billing_period_start": "2026-06-01",
            "billing_period_end": "2026-06-30",
            "amount_gross": 100.0,
            "vat_rate": 0.21,
            "currency": "EUR",
            "payment_status": "paid",
            "source": "synthetic",
            "created_at": "2026-07-01T00:00:00Z",
        }
    )


def test_no_emission_before_definition_and_rule_arrive():
    fn = CostDefinitionResolutionFunction()
    ctx = FakeMultiState()
    results = list(
        fn.process_element(
            _payment_line("00000000-0000-0000-0000-000000000001"), ctx
        )
    )
    assert results == []


def test_emits_enriched_payment_line_once_both_broadcasts_arrive():
    fn = CostDefinitionResolutionFunction()
    ctx = FakeMultiState()
    cost_definition_id = "00000000-0000-0000-0000-000000000001"

    fn.process_broadcast_element(
        CostDefinitionRow(
            cost_definition_id=cost_definition_id,
            concept="electricity",
            scope="property",
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
            method="calendar_day",
            weight_config=None,
        ),
        ctx,
    )

    results = list(fn.process_element(_payment_line(cost_definition_id), ctx))

    assert len(results) == 1
    enriched = results[0]
    assert enriched.concept == "electricity"
    assert enriched.behavior == "variable"
    assert enriched.allocation_method == "calendar_day"
    assert enriched.amount_gross == 100.0
    assert enriched.billing_period_start == date(2026, 6, 1)


def test_no_emission_when_only_definition_arrives():
    fn = CostDefinitionResolutionFunction()
    ctx = FakeMultiState()
    cost_definition_id = "00000000-0000-0000-0000-000000000001"
    fn.process_broadcast_element(
        CostDefinitionRow(
            cost_definition_id=cost_definition_id,
            concept="electricity",
            scope="property",
            behavior="variable",
            trigger="time",
            calculation_base="fixed_amount",
            recurrence="monthly",
            revenue_base=None,
        ),
        ctx,
    )
    results = list(fn.process_element(_payment_line(cost_definition_id), ctx))
    assert results == []
