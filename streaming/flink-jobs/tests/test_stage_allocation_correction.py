from datetime import UTC, date, datetime

from flink_jobs.cost_aggregation import PendingAllocationCorrection
from flink_jobs.models import CostAggregate
from flink_jobs.stage_allocation_correction import AllocationCorrectionFunction


def _cost(pending=(), occupied_nights=0, booking_count=0, available_days=30):
    return CostAggregate(
        apartment_id="BCN-001",
        apartment_reference="BCN-001",
        city="Barcelona",
        neighborhood="Eixample",
        property_type="studio",
        bedrooms=0,
        fixed_cost_eur=10.0,
        variable_cost_eur=5.0,
        one_time_cost_eur=0.0,
        total_monthly_cost_eur=450.0,
        available_days=available_days,
        cost_lines_count=1,
        billing_period_start=date(2026, 6, 1),
        billing_period_end=date(2026, 6, 30),
        target_margin=0.05,
        competitiveness_discount=0.05,
        updated_at=datetime.now(UTC),
        occupied_nights=occupied_nights,
        booking_count=booking_count,
        pending_allocation_corrections=pending,
    )


def test_passthrough_when_no_pending_corrections():
    fn = AllocationCorrectionFunction()
    result = fn.map(_cost())
    assert result.fixed_cost_eur == 10.0
    assert result.variable_cost_eur == 5.0


def test_occupied_night_correction_added_to_variable_bucket():
    pending = (
        PendingAllocationCorrection(
            concept="cleaning",
            behavior="variable",
            allocation_method="occupied_night",
            monthly_equivalent_eur=300.0,
        ),
    )
    fn = AllocationCorrectionFunction()
    result = fn.map(_cost(pending=pending, occupied_nights=10))
    assert result.variable_cost_eur == 5.0 + 30.0  # 300 / 10
    assert result.pending_allocation_corrections == ()


def test_booking_correction_added_to_fixed_bucket():
    pending = (
        PendingAllocationCorrection(
            concept="other",
            behavior="fixed",
            allocation_method="booking",
            monthly_equivalent_eur=150.0,
        ),
    )
    fn = AllocationCorrectionFunction()
    result = fn.map(_cost(pending=pending, booking_count=5))
    assert result.fixed_cost_eur == 10.0 + 30.0  # 150 / 5


def test_zero_denominator_falls_back_to_available_days():
    pending = (
        PendingAllocationCorrection(
            concept="cleaning",
            behavior="variable",
            allocation_method="occupied_night",
            monthly_equivalent_eur=300.0,
        ),
    )
    fn = AllocationCorrectionFunction()
    result = fn.map(_cost(pending=pending, occupied_nights=0, available_days=30))
    assert result.variable_cost_eur == 5.0 + 10.0  # 300 / 30 (fallback)


def test_multiple_corrections_accumulate():
    pending = (
        PendingAllocationCorrection(
            concept="cleaning",
            behavior="variable",
            allocation_method="occupied_night",
            monthly_equivalent_eur=300.0,
        ),
        PendingAllocationCorrection(
            concept="other",
            behavior="fixed",
            allocation_method="booking",
            monthly_equivalent_eur=150.0,
        ),
    )
    fn = AllocationCorrectionFunction()
    result = fn.map(_cost(pending=pending, occupied_nights=10, booking_count=5))
    assert result.variable_cost_eur == 5.0 + 30.0
    assert result.fixed_cost_eur == 10.0 + 30.0
