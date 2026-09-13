import json

from flink_jobs.job import (
    _parse_apartment_segment_row,
    _parse_company_cost_occurrence_row,
    _parse_cost_allocation_rule_row,
    _parse_cost_definition_row,
    _parse_owner_contract_row,
)

_BASE_SEGMENT_ROW = {
    "apartment_id": "BCN-001",
    "city": "Barcelona",
    "neighborhood": "Eixample",
    "property_type": "studio",
    "bedrooms": 0,
    "target_margin": 0.05,
    "competitiveness_discount": 0.05,
}

_BASE_OWNER_CONTRACT_ROW = {
    "apartment_id": "BCN-001",
    "owner_id": "OWN-001",
}


def test_parses_apartment_segment_row():
    # Phase 11 (ADR-0011 backlog #5): apartment_market_segments no longer
    # carries commission_pct at all — nothing left to default here.
    row = _parse_apartment_segment_row(json.dumps(_BASE_SEGMENT_ROW))
    assert row.apartment_id == "BCN-001"
    assert row.target_margin == 0.05


def test_parses_commission_base_and_pct_when_present():
    row = _parse_owner_contract_row(
        json.dumps(
            {
                **_BASE_OWNER_CONTRACT_ROW,
                "commission_base": "revenue_minus_ota",
                "commission_pct": 0.2,
            }
        )
    )
    assert row.commission_base == "revenue_minus_ota"
    assert row.commission_pct == 0.2


def test_defaults_commission_base_and_pct_for_historical_messages():
    # Same defensive-default pattern ADR-0009 established for
    # apartment_market_segments' own commission_pct, now applied to
    # owner_contracts (Phase 11).
    row = _parse_owner_contract_row(json.dumps(_BASE_OWNER_CONTRACT_ROW))
    assert row.commission_base == "total_revenue"
    assert row.commission_pct == 0.15


def test_parses_cost_definition_row():
    row = _parse_cost_definition_row(
        json.dumps(
            {
                "cost_definition_id": "00000000-0000-0000-0000-000000000001",
                "concept": "office_rent",
                "scope": "company",
                "behavior": "fixed",
                "trigger": "time",
                "calculation_base": "fixed_amount",
                "recurrence": "monthly",
                "revenue_base": None,
            }
        )
    )
    assert row.concept == "office_rent"
    assert row.scope == "company"


def test_parses_cost_allocation_rule_row():
    row = _parse_cost_allocation_rule_row(
        json.dumps(
            {
                "cost_definition_id": "00000000-0000-0000-0000-000000000001",
                "method": "weighted",
                "weight_config": '{"BCN-001": 1.0}',
            }
        )
    )
    assert row.method == "weighted"
    assert row.weight_config == '{"BCN-001": 1.0}'


def test_parses_company_cost_occurrence_row():
    row = _parse_company_cost_occurrence_row(
        json.dumps(
            {
                "company_cost_occurrence_id": "00000000-0000-0000-0000-000000000002",
                "cost_definition_id": "00000000-0000-0000-0000-000000000001",
                "billing_period_start": "2026-06-01",
                "billing_period_end": "2026-06-30",
                "amount_gross": 1200.0,
            }
        )
    )
    assert row.amount_gross == 1200.0
