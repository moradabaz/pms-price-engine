from datetime import UTC, date, datetime
from uuid import uuid4

from flink_jobs.dynamodb_sink import _python_to_dynamodb, _to_dynamodb_item
from shared_schemas.price_decision import (
    BillingPeriod,
    Calculation,
    CostInputs,
    LosFloorCandidate,
    MarketInputs,
    Output,
    PriceDecision,
)


def _decision() -> PriceDecision:
    return PriceDecision(
        decision_id=uuid4(),
        apartment_id="BCN-001",
        apartment_reference="BCN-001",
        target_date=date(2026, 9, 1),
        decided_at=datetime.now(UTC),
        cost_inputs=CostInputs(
            billing_period=BillingPeriod(start=date(2026, 8, 1), end=date(2026, 8, 31)),
            total_monthly_cost_eur=100.0,
            available_days=31,
            fixed_cost_eur=1.0,
            variable_cost_eur=1.0,
            one_time_cost_eur=10.0,
        ),
        market_inputs=MarketInputs(
            market_area="Barcelona/Eixample",
            avg_nightly_rate_eur=100.0,
            collected_at=datetime.now(UTC),
            data_age_seconds=0,
        ),
        calculation=Calculation(
            target_margin=0.05,
            minimum_price_eur=50.0,
            floor_type="structural_full_margin",
            commission_pct=0.15,
            days_to_arrival=45,
            competitiveness_discount=0.05,
            property_attribute_factor=1.0,
            property_reference_price_eur=100.0,
            market_reference_price_eur=95.0,
            rule_applied="market_competitive",
            los_floor_matrix=[
                LosFloorCandidate(
                    stay_length=n,
                    minimum_price_eur=50.0,
                    floor_type="structural_full_margin",
                    rule_applied="market_competitive",
                    suggested_price_eur=95.0,
                    effective_margin=0.9,
                )
                for n in (1, 2, 3, 7, 14)
            ],
        ),
        output=Output(
            suggested_price_eur=95.0, effective_margin=0.9, below_market_by=5.0
        ),
    )


def test_python_to_dynamodb_serializes_lists_as_list_type():
    # Regression test: _python_to_dynamodb had no `list` case before Phase 9
    # (ADR-0011 backlog #1) — every decision with a non-empty los_floor_matrix
    # raised TypeError at serialization time, silently dropping every write
    # (caught live against LocalStack, not by the unit suite — no test
    # exercised this function's list handling at all until this one).
    assert _python_to_dynamodb([1, 2.0, "x"]) == {
        "L": [{"N": "1"}, {"N": "2.0"}, {"S": "x"}]
    }


def test_to_dynamodb_item_serializes_los_floor_matrix_without_raising():
    item = _to_dynamodb_item(_decision())
    matrix = item["calculation"]["M"]["los_floor_matrix"]["L"]
    assert len(matrix) == 5
    assert matrix[0]["M"]["stay_length"] == {"N": "1"}
    assert matrix[0]["M"]["rule_applied"] == {"S": "market_competitive"}
