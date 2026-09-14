from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

# Phase 11 (ADR-0011 backlog #5, spec 11 §4): the minimal slice of backlog
# #3 (reading PaymentLine.concept) a netted commission base needs — not a
# general 13-concept breakdown, just enough to net "OTA-related" and
# "cleaning" amounts out of the commission base in pricing.py.
OTA_RELATED_CONCEPTS = frozenset({"ota_fee", "channel_manager"})
CLEANING_CONCEPTS = frozenset({"cleaning"})
# Phase 24 (ADR-0017 §3): the external spec's 4th revenue base
# (revenue_minus_ota_minus_cleaning_minus_laundry, §11.1) needs a
# laundry-only sub-total, same pattern CLEANING_CONCEPTS already
# establishes.
LAUNDRY_CONCEPTS = frozenset({"laundry"})

# Phase 17 (ADR-0011 backlog #3): the remaining 10 of 13 concept values,
# which fall through undifferentiated into fixed_cost_eur/variable_cost_eur
# today — canonical display order for cost_breakdown, matching
# payment_line.v1's own enum order (specs/events/payment_line.v1.json).
CONCEPT_ORDER = (
    "electricity",
    "water",
    "gas",
    "internet",
    "pms_subscription",
    "ota_fee",
    "channel_manager",
    "office_rent",
    "cleaning",
    "laundry",
    "maintenance",
    "insurance",
    "community_fee",
    "other",
)

# Phase 19 (ADR-0011 backlog #13): a recurring cost's amount_gross is spread
# over this many months before any per-night allocation runs — the direct
# fix for the "annual insurance lump sum" problem (an annual premium no
# longer spikes fixed_cost_eur in whichever single month it happens to be
# recorded). one_off lines never go through this divisor — they use the
# same "one turnover" averaging one_time_cost_eur always has (see below).
_RECURRENCE_MONTHLY_DIVISOR = {
    "per_booking": 1,
    "daily": 1,
    "monthly": 1,
    "quarterly": 3,
    "annual": 12,
}

# Phase 19: allocation methods computable immediately in Stage A, using only
# data already available here (available_days). occupied_night/booking need
# Phase 18 data not available until later in the pipeline — those lines are
# excluded from fixed_cost_eur/variable_cost_eur here and instead surface as
# a PendingAllocationCorrection, resolved by stage_allocation_correction.py.
_IMMEDIATE_ALLOCATION_METHODS = frozenset(
    {"direct", "calendar_day", "available_night", "weighted", "revenue"}
)

# Phase 20 (ADR-0013 §3): a CostDefinition is percentage-based iff its
# calculation_base is one of these two — such lines never contribute an EUR
# amount to fixed_cost_eur/variable_cost_eur (their real amount_gross is
# historical, not the prospective figure the Break-Even/Profitable Floor
# formula needs); only their CostDefinition.rate matters going forward.
_PERCENTAGE_CALCULATION_BASES = frozenset({"pct_revenue", "pct_adjusted_revenue"})


@dataclass(frozen=True)
class ConceptAmount:
    concept: str
    amount_eur: float
    # Phase 20 (ADR-0013, spec 20 §2): the CostDefinition's own dimensions,
    # alongside the concept/amount above — display-only, mirrors
    # price_decision.v1's own cost_breakdown shape field-for-field.
    scope: str
    behavior: str
    trigger: str
    calculation_base: str
    recurrence: str
    allocation_method: str


@dataclass(frozen=True)
class PercentageCostComponent:
    """One percentage CostDefinition applicable to this apartment/period
    (Phase 20, ADR-0013 §3) — contributes `rate` to the Break-Even/
    Profitable Floor formula's `p` term, and (via revenue_base) to the
    generalized revenue-base netting stage_price_decision.py computes.
    Never carries an EUR amount — CostOccurrence's real invoiced figure for
    such a line is historical, not the prospective rate the formula needs."""

    concept: str
    rate: float
    revenue_base: str | None


@dataclass(frozen=True)
class EnrichedPaymentLine:
    """A PaymentLine with its CostDefinition/CostAllocationRule already
    resolved (Phase 19, Stage A0) — everything aggregate_cost() needs,
    without re-reading either broadcast state itself. Defined here (not
    models.py) since models.py already imports ConceptAmount from this
    module — importing the other way would be circular."""

    event_id: str
    apartment_id: str
    apartment_reference: str
    billing_period_start: date
    billing_period_end: date
    amount_gross: float
    concept: str
    scope: str
    behavior: str
    trigger: str
    calculation_base: str
    recurrence: str
    revenue_base: str | None
    allocation_method: str
    weight_config: str | None
    # Phase 20 (ADR-0013 §3): only meaningful when calculation_base is
    # percentage-based (enforced at the DB level by cost_definitions'
    # own pairing CHECK constraint).
    rate: float | None = None


@dataclass(frozen=True)
class PendingAllocationCorrection:
    """One concept's monthly-equivalent amount, still needing division by a
    figure only available after Phase 18's booking stage (occupied_nights,
    booking_count) — Stage A computed it with a calendar_day placeholder
    already excluded from fixed_cost_eur/variable_cost_eur; Stage
    A-correction (spec 19 §4) divides it correctly and adds it in."""

    concept: str
    behavior: str
    allocation_method: str
    monthly_equivalent_eur: float


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
    # Renamed from one_time_cost_eur (Phase 20, ADR-0013) — still an average
    # (not sum) of allocation_method='booking' lines, unchanged mechanically.
    per_booking_cost_eur: float
    ota_related_cost_eur: float
    cleaning_cost_eur: float
    # Phase 24 (ADR-0017 §3): laundry_cost_eur mirrors cleaning_cost_eur;
    # booking_scope_cost_eur is the first aggregate in this project keyed by
    # CostDefinition.scope rather than .behavior — every scope='booking'
    # line's contribution for the period, the widest of the 5 revenue-base
    # netting amounts by construction (ADR-0017 §2).
    laundry_cost_eur: float
    booking_scope_cost_eur: float
    cost_breakdown: tuple[ConceptAmount, ...]
    pending_allocation_corrections: tuple[PendingAllocationCorrection, ...]
    # Phase 20 (ADR-0013 §3): one entry per distinct percentage CostDefinition
    # concept observed this period — feeds `p` downstream (stage_price_
    # decision.py), never an EUR amount here.
    percentage_costs: tuple[PercentageCostComponent, ...]


def aggregate_cost(
    payment_lines: Iterable[EnrichedPaymentLine],
) -> CostAggregationResult | None:
    """Splits lines sharing the latest billing_period_end. Phase 19 (ADR-0011
    backlog #13): grouping now reads CostDefinition.behavior/recurrence
    (resolved by Stage A0), not PaymentLine.cost_type directly — numerically
    a drop-in replacement of the old cost_type-based split once the backfill
    migration has run, since every line's behavior/recurrence was seeded 1:1
    from its old cost_type (semi_variable folds into the variable bucket;
    one_off lines are identified by recurrence, not behavior — see
    ADR-0012). Returns the aggregation result, or None if payment_lines is
    empty."""
    lines = list(payment_lines)
    if not lines:
        return None

    current_end = max(line.billing_period_end for line in lines)
    matching = [line for line in lines if line.billing_period_end == current_end]
    period_start = matching[0].billing_period_start
    total = round(sum(line.amount_gross for line in matching), 2)
    available_days = (current_end - period_start).days + 1

    # Phase 20 (ADR-0013 §3): percentage-based lines never contribute an EUR
    # amount to fixed/variable/per-booking — their real amount_gross is
    # historical, not the prospective rate the Break-Even/Profitable Floor
    # formula needs. Excluded from every bucket below; handled separately.
    percentage_lines = [
        line
        for line in matching
        if line.calculation_base in _PERCENTAGE_CALCULATION_BASES
    ]
    non_percentage_lines = [
        line
        for line in matching
        if line.calculation_base not in _PERCENTAGE_CALCULATION_BASES
    ]

    one_time_lines = [
        line for line in non_percentage_lines if line.recurrence == "one_off"
    ]
    recurring_lines = [
        line for line in non_percentage_lines if line.recurrence != "one_off"
    ]

    immediate_recurring = [
        line
        for line in recurring_lines
        if line.allocation_method in _IMMEDIATE_ALLOCATION_METHODS
    ]
    deferred_recurring = [
        line
        for line in recurring_lines
        if line.allocation_method not in _IMMEDIATE_ALLOCATION_METHODS
    ]

    def _monthly_equivalent(line: EnrichedPaymentLine) -> float:
        divisor = _RECURRENCE_MONTHLY_DIVISOR.get(line.recurrence, 1)
        return line.amount_gross / divisor

    fixed_total = sum(
        _monthly_equivalent(line)
        for line in immediate_recurring
        if line.behavior == "fixed"
    )
    variable_total = sum(
        _monthly_equivalent(line)
        for line in immediate_recurring
        if line.behavior in ("variable", "semi_variable")
    )

    fixed_cost_eur = (
        round(fixed_total / available_days, 2) if available_days > 0 else 0.0
    )
    variable_cost_eur = (
        round(variable_total / available_days, 2) if available_days > 0 else 0.0
    )
    # Averaged, not summed: a one_time line is already "the cost of one
    # turnover" (e.g. one cleaning invoice) — summing a period's worth would
    # conflate one reservation's cost with the whole period's (ADR-0009 D3).
    per_booking_cost_eur = (
        round(
            sum(line.amount_gross for line in one_time_lines) / len(one_time_lines), 2
        )
        if one_time_lines
        else 0.0
    )

    # Phase 20 (ADR-0013 §3): one PercentageCostComponent per distinct
    # concept among percentage_lines — rate is a CostDefinition-level
    # constant (same for every line sharing that cost_definition_id), so it
    # is taken once per concept, never summed across lines.
    percentage_concepts = {line.concept for line in percentage_lines}
    percentage_costs = tuple(
        PercentageCostComponent(
            concept=concept,
            rate=next(line.rate for line in percentage_lines if line.concept == concept)
            or 0.0,
            revenue_base=next(
                line.revenue_base
                for line in percentage_lines
                if line.concept == concept
            ),
        )
        for concept in percentage_concepts
    )

    # Phase 19: one PendingAllocationCorrection per concept among the
    # deferred lines (occupied_night/booking) — grouped and summed the same
    # way fixed/variable totals are, just not divided by available_days yet.
    pending_concepts = {line.concept for line in deferred_recurring}
    pending_allocation_corrections = tuple(
        PendingAllocationCorrection(
            concept=concept,
            behavior=next(
                line.behavior for line in deferred_recurring if line.concept == concept
            ),
            allocation_method=next(
                line.allocation_method
                for line in deferred_recurring
                if line.concept == concept
            ),
            monthly_equivalent_eur=round(
                sum(
                    _monthly_equivalent(line)
                    for line in deferred_recurring
                    if line.concept == concept
                ),
                2,
            ),
        )
        for concept in pending_concepts
    )

    # Phase 11 (ADR-0011 backlog #5): both amounts are already fully counted
    # inside fixed_cost_eur/variable_cost_eur above (ota_fee/cleaning are
    # cost_type=variable, channel_manager is cost_type=fixed in mock-pm-app's
    # synthetic data) — these are additional breakdowns for pricing.py's
    # commission-base netting, not new costs. Phase 19: computed over every
    # matching line regardless of allocation_method (deferred lines' true
    # per-night value isn't known yet, but their calendar_day-equivalent
    # netting figure is a reasonable placeholder for this purpose, same
    # spirit as cost_breakdown below).
    ota_related_total = sum(
        line.amount_gross for line in matching if line.concept in OTA_RELATED_CONCEPTS
    )
    cleaning_total = sum(
        line.amount_gross for line in matching if line.concept in CLEANING_CONCEPTS
    )
    # Phase 24 (ADR-0017 §3): laundry mirrors ota_related/cleaning exactly.
    laundry_total = sum(
        line.amount_gross for line in matching if line.concept in LAUNDRY_CONCEPTS
    )
    # Phase 24 (ADR-0017 §2): every matching line whose CostDefinition is
    # booking-scoped, regardless of concept — the widest netting base.
    booking_scope_total = sum(
        line.amount_gross for line in matching if line.scope == "booking"
    )
    ota_related_cost_eur = (
        round(ota_related_total / available_days, 2) if available_days > 0 else 0.0
    )
    cleaning_cost_eur = (
        round(cleaning_total / available_days, 2) if available_days > 0 else 0.0
    )
    laundry_cost_eur = (
        round(laundry_total / available_days, 2) if available_days > 0 else 0.0
    )
    booking_scope_cost_eur = (
        round(booking_scope_total / available_days, 2) if available_days > 0 else 0.0
    )

    # Phase 17 (ADR-0011 backlog #3): every concept observed in the period,
    # not just the 2 (ota_fee/channel_manager, cleaning) Phase 11 already
    # split out — reporting granularity, same per-day averaging convention
    # as fixed_cost_eur/ota_related_cost_eur above. Only concepts with at
    # least one matching line appear (never a fabricated zero entry).
    concepts_present = {line.concept for line in matching}
    cost_breakdown = tuple(
        ConceptAmount(
            concept=concept,
            amount_eur=round(
                sum(line.amount_gross for line in matching if line.concept == concept)
                / available_days,
                2,
            )
            if available_days > 0
            else 0.0,
            # Phase 20 (ADR-0013, spec 20 §2): every line sharing a concept
            # shares the same CostDefinition (Stage A0 resolved it), so its
            # dimensions are taken from any one matching line.
            scope=next(line.scope for line in matching if line.concept == concept),
            behavior=next(
                line.behavior for line in matching if line.concept == concept
            ),
            trigger=next(line.trigger for line in matching if line.concept == concept),
            calculation_base=next(
                line.calculation_base for line in matching if line.concept == concept
            ),
            recurrence=next(
                line.recurrence for line in matching if line.concept == concept
            ),
            allocation_method=next(
                line.allocation_method for line in matching if line.concept == concept
            ),
        )
        for concept in CONCEPT_ORDER
        if concept in concepts_present
    )

    return CostAggregationResult(
        billing_period_start=period_start,
        billing_period_end=current_end,
        total_monthly_cost_eur=total,
        available_days=available_days,
        cost_lines_count=len(matching),
        fixed_cost_eur=fixed_cost_eur,
        variable_cost_eur=variable_cost_eur,
        per_booking_cost_eur=per_booking_cost_eur,
        percentage_costs=percentage_costs,
        ota_related_cost_eur=ota_related_cost_eur,
        cleaning_cost_eur=cleaning_cost_eur,
        laundry_cost_eur=laundry_cost_eur,
        booking_scope_cost_eur=booking_scope_cost_eur,
        cost_breakdown=cost_breakdown,
        pending_allocation_corrections=pending_allocation_corrections,
    )
