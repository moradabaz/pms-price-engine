from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from shared_schemas.payment_line import PaymentLine


def retained_billing_period_ends(
    period_ends: Iterable[date], keep: int = 2
) -> set[date]:
    """Returns the `keep` most recent distinct billing_period_end values."""
    distinct_sorted = sorted(set(period_ends), reverse=True)
    return set(distinct_sorted[:keep])


@dataclass(frozen=True)
class CostAggregationResult:
    billing_period_start: date
    billing_period_end: date
    total_monthly_cost_eur: float
    available_days: int
    cost_lines_count: int
    daily_cost_eur: float


def aggregate_cost(
    payment_lines: Iterable[PaymentLine],
) -> CostAggregationResult | None:
    """Sums amount_gross across lines sharing the latest billing_period_end.
    Returns the aggregation result, or None if payment_lines is empty."""
    lines = list(payment_lines)
    if not lines:
        return None

    current_end = max(line.billing_period_end for line in lines)
    matching = [line for line in lines if line.billing_period_end == current_end]
    period_start = matching[0].billing_period_start
    total = round(sum(line.amount_gross for line in matching), 2)
    available_days = (current_end - period_start).days + 1
    daily_cost_eur = round(total / available_days, 2) if available_days > 0 else 0.0

    return CostAggregationResult(
        billing_period_start=period_start,
        billing_period_end=current_end,
        total_monthly_cost_eur=total,
        available_days=available_days,
        cost_lines_count=len(matching),
        daily_cost_eur=daily_cost_eur,
    )
