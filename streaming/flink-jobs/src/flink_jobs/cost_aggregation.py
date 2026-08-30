from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from shared_schemas.payment_line import PaymentLine

# Phase 11 (ADR-0011 backlog #5, spec 11 §4): the minimal slice of backlog
# #3 (reading PaymentLine.concept) a netted commission base needs — not a
# general 13-concept breakdown, just enough to net "OTA-related" and
# "cleaning" amounts out of the commission base in pricing.py.
OTA_RELATED_CONCEPTS = frozenset({"ota_fee", "channel_manager"})
CLEANING_CONCEPTS = frozenset({"cleaning"})


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
    fixed_cost_eur: float
    variable_cost_eur: float
    one_time_cost_eur: float
    ota_related_cost_eur: float
    cleaning_cost_eur: float


def aggregate_cost(
    payment_lines: Iterable[PaymentLine],
) -> CostAggregationResult | None:
    """Splits lines sharing the latest billing_period_end by cost_type
    (ADR-0009). Returns the aggregation result, or None if payment_lines is
    empty."""
    lines = list(payment_lines)
    if not lines:
        return None

    current_end = max(line.billing_period_end for line in lines)
    matching = [line for line in lines if line.billing_period_end == current_end]
    period_start = matching[0].billing_period_start
    total = round(sum(line.amount_gross for line in matching), 2)
    available_days = (current_end - period_start).days + 1

    fixed_total = sum(
        line.amount_gross for line in matching if line.cost_type == "fixed"
    )
    variable_total = sum(
        line.amount_gross for line in matching if line.cost_type == "variable"
    )
    one_time_lines = [line for line in matching if line.cost_type == "one_time"]

    fixed_cost_eur = (
        round(fixed_total / available_days, 2) if available_days > 0 else 0.0
    )
    variable_cost_eur = (
        round(variable_total / available_days, 2) if available_days > 0 else 0.0
    )
    # Averaged, not summed: a one_time line is already "the cost of one
    # turnover" (e.g. one cleaning invoice) — summing a period's worth would
    # conflate one reservation's cost with the whole period's (ADR-0009 D3).
    one_time_cost_eur = (
        round(
            sum(line.amount_gross for line in one_time_lines) / len(one_time_lines), 2
        )
        if one_time_lines
        else 0.0
    )

    # Phase 11 (ADR-0011 backlog #5): both amounts are already fully counted
    # inside fixed_cost_eur/variable_cost_eur above (ota_fee/cleaning are
    # cost_type=variable, channel_manager is cost_type=fixed in mock-pm-app's
    # synthetic data) — these are additional breakdowns for pricing.py's
    # commission-base netting, not new costs.
    ota_related_total = sum(
        line.amount_gross for line in matching if line.concept in OTA_RELATED_CONCEPTS
    )
    cleaning_total = sum(
        line.amount_gross for line in matching if line.concept in CLEANING_CONCEPTS
    )
    ota_related_cost_eur = (
        round(ota_related_total / available_days, 2) if available_days > 0 else 0.0
    )
    cleaning_cost_eur = (
        round(cleaning_total / available_days, 2) if available_days > 0 else 0.0
    )

    return CostAggregationResult(
        billing_period_start=period_start,
        billing_period_end=current_end,
        total_monthly_cost_eur=total,
        available_days=available_days,
        cost_lines_count=len(matching),
        fixed_cost_eur=fixed_cost_eur,
        variable_cost_eur=variable_cost_eur,
        one_time_cost_eur=one_time_cost_eur,
        ota_related_cost_eur=ota_related_cost_eur,
        cleaning_cost_eur=cleaning_cost_eur,
    )
