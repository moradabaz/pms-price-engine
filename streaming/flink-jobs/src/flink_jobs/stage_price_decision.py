import logging
from datetime import UTC, date, datetime
from typing import cast
from uuid import uuid4

from pricing_formulas.engine import (
    decide_price,
    decide_price_by_channel,
    decide_price_los_matrix,
    recommend_minimum_stay,
)
from pricing_formulas.layers.commercial import (
    RevenueBase,
    netted_revenue_base_amount,
    revenue_base_netting_component,
)
from pricing_formulas.viability import classify_viability
from pyflink.common.typeinfo import Types
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import KeyedCoProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from shared_schemas.market_price import MarketPrice
from shared_schemas.price_decision import (
    BillingPeriod,
    Calculation,
    ChannelPriceCandidate,
    CostConceptAmount,
    CostInputs,
    DecisionComponent,
    LosFloorCandidate,
    MarketInputs,
    MinimumStayRecommendation,
    Output,
    PriceDecision,
)

from flink_jobs.cost_aggregation import PercentageCostComponent
from flink_jobs.eviction import (
    expired_night_keys,
    is_over_capacity,
    oldest_key_by_updated_at,
)
from flink_jobs.models import (
    CostAggregate,
    MarketSnapshot,
    NightSnapshot,
    StayCandidate,
)
from flink_jobs.staleness import is_safe_to_overwrite
from flink_jobs.watchdog import expired_keys, next_deadline_millis

logger = logging.getLogger(__name__)

DATA_STALE_TAG = OutputTag("data-stale", Types.PICKLED_BYTE_ARRAY())

APARTMENTS_DESCRIPTOR = MapStateDescriptor(
    "apartments-in-segment", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
NIGHTS_DESCRIPTOR = MapStateDescriptor(
    "nights-in-segment", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
APARTMENT_DEADLINES_DESCRIPTOR = MapStateDescriptor(
    "apartment-deadlines", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
NIGHT_DEADLINES_DESCRIPTOR = MapStateDescriptor(
    "night-deadlines", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)
# Phase 23 (ADR-0016 §4, spec 23 §7): keyed by "{apartment_id}|{target_date}"
# — NOT on NightSnapshot, which spec 23 originally proposed. NightSnapshot
# (self.nights) is keyed by target_date alone and shared across every
# apartment in this segment, so a field on it cannot hold a per-apartment
# breach streak without wrongly sharing one apartment's streak with every
# other apartment priced against the same night. A genuinely new state,
# scoped to the actual (apartment, night) pair, is required.
FLOOR_BREACH_DESCRIPTOR = MapStateDescriptor(
    "floor-breach-since", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
)


def _floor_breach_key(apartment_id: str, target_date: date) -> str:
    return f"{apartment_id}|{target_date.isoformat()}"


class PriceDecisionFunction(KeyedCoProcessFunction):
    """Stage B: cross-joins known apartments and known nights within one
    segment, computing a price_decision on every relevant change. Emits
    PriceDecision on the main output, DataStale alerts on a side output."""

    def open(self, runtime_context):
        self.apartments = runtime_context.get_map_state(APARTMENTS_DESCRIPTOR)
        self.nights = runtime_context.get_map_state(NIGHTS_DESCRIPTOR)
        self.apartment_deadlines = runtime_context.get_map_state(
            APARTMENT_DEADLINES_DESCRIPTOR
        )
        self.night_deadlines = runtime_context.get_map_state(NIGHT_DEADLINES_DESCRIPTOR)
        self.floor_breach_since = runtime_context.get_map_state(FLOOR_BREACH_DESCRIPTOR)

    def process_element1(self, value: CostAggregate, ctx):
        """Cost side: updates one apartment, then reprices every known night."""
        existing = self.apartments.get(value.apartment_id)
        if not is_safe_to_overwrite(
            value.updated_at, existing.updated_at if existing else None
        ):
            return

        if is_over_capacity(
            len(dict(self.apartments.items())) + (0 if existing else 1)
        ):
            oldest = oldest_key_by_updated_at(dict(self.apartments.items()))
            self.apartments.remove(oldest)
            self.apartment_deadlines.remove(oldest)
            # Phase 23: an evicted apartment's floor-breach streaks are no
            # longer meaningful — drop every (apartment, night) entry for it.
            for key in list(dict(self.floor_breach_since.items())):
                if key.startswith(f"{oldest}|"):
                    self.floor_breach_since.remove(key)

        self.apartments.put(value.apartment_id, value)
        deadline_millis = next_deadline_millis(datetime.now(UTC))
        self.apartment_deadlines.put(value.apartment_id, deadline_millis)
        ctx.timer_service().register_processing_time_timer(deadline_millis)

        for target_date_str, night in dict(self.nights.items()).items():
            # Phase 16 (ADR-0011 backlog #2): a night isn't "known" until its
            # blended snapshot exists — a channel-only update for a
            # brand-new night is stored (process_element2) but doesn't
            # fan out from here either.
            if night.blended is None:
                continue
            target_date = date.fromisoformat(target_date_str)
            key = _floor_breach_key(value.apartment_id, target_date)
            decision, new_since = _build_price_decision(
                value, night, target_date, self.floor_breach_since.get(key)
            )
            if new_since is None:
                self.floor_breach_since.remove(key)
            else:
                self.floor_breach_since.put(key, new_since)
            yield decision

    def process_element2(self, value: MarketPrice, ctx):
        """Market side: updates one night (its blended rate, or one channel's
        rate — Phase 16, ADR-0011 backlog #2), then reprices every known
        apartment, provided the night's blended rate is already known."""
        target_date = value.target_date
        if target_date < datetime.now(UTC).date():
            return

        key = target_date.isoformat()
        platform = value.market_context.platform
        neighborhood = value.market_area.neighborhood or ""
        market_area = f"{value.market_area.city}/{neighborhood}".rstrip("/")
        snapshot = MarketSnapshot(
            market_area=market_area,
            avg_nightly_rate_eur=value.pricing.avg_nightly_rate,
            occupancy_rate=value.market_context.occupancy_rate,
            sample_size=value.market_context.sample_size,
            collected_at=value.collected_at,
        )
        existing = self.nights.get(key)
        # Phase 16: staleness is checked against this specific sub-snapshot
        # (the blended one, or this one channel's) — not the whole
        # NightSnapshot, so an old blended update can't be blocked by a
        # fresher channel update for the same night, or vice versa.
        previous_collected_at = None
        if existing is not None:
            previous = (
                existing.blended
                if platform is None
                else existing.channels.get(platform)
            )
            previous_collected_at = previous.collected_at if previous else None
        if not is_safe_to_overwrite(snapshot.collected_at, previous_collected_at):
            return

        expired = expired_night_keys(
            {k: date.fromisoformat(k) for k in dict(self.nights.items())},
            today=datetime.now(UTC).date(),
        )
        for expired_key in expired:
            self.nights.remove(expired_key)
            self.night_deadlines.remove(expired_key)
            # Phase 23: an expired night's floor-breach streaks are no
            # longer meaningful for any apartment.
            for breach_key in list(dict(self.floor_breach_since.items())):
                if breach_key.endswith(f"|{expired_key}"):
                    self.floor_breach_since.remove(breach_key)

        if platform is None:
            night = NightSnapshot(
                blended=snapshot,
                channels=dict(existing.channels) if existing else {},
            )
        else:
            channels = dict(existing.channels) if existing else {}
            channels[platform] = snapshot
            night = NightSnapshot(
                blended=existing.blended if existing else None, channels=channels
            )

        self.nights.put(key, night)
        deadline_millis = next_deadline_millis(datetime.now(UTC))
        self.night_deadlines.put(key, deadline_millis)
        ctx.timer_service().register_processing_time_timer(deadline_millis)

        if night.blended is None:
            return
        for apartment_id, cost in dict(self.apartments.items()).items():
            breach_key = _floor_breach_key(apartment_id, target_date)
            decision, new_since = _build_price_decision(
                cost, night, target_date, self.floor_breach_since.get(breach_key)
            )
            if new_since is None:
                self.floor_breach_since.remove(breach_key)
            else:
                self.floor_breach_since.put(breach_key, new_since)
            yield decision

    def on_timer(self, timestamp: int, ctx):
        """Fires data_stale for apartments/nights whose deadline matches timestamp."""
        for apartment_id in expired_keys(
            dict(self.apartment_deadlines.items()), timestamp
        ):
            yield DATA_STALE_TAG, ("apartment", apartment_id)
        for night_key in expired_keys(dict(self.night_deadlines.items()), timestamp):
            yield DATA_STALE_TAG, ("night", night_key)


def _build_price_decision(
    cost: CostAggregate,
    night: NightSnapshot,
    target_date: date,
    floor_breach_since: date | None,
) -> tuple[PriceDecision, date | None]:
    """Applies the pricing formula and assembles a PriceDecision. Returns it
    alongside this (apartment, night)'s updated floor_breach_since (Phase 23,
    ADR-0016 §4) — the caller is responsible for writing it back to its own
    FLOOR_BREACH_DESCRIPTOR state under the right composite key, since that
    state lives on the caller (a KeyedCoProcessFunction), not here.
    night.blended must already be resolved (Phase 16, ADR-0011 backlog #2) —
    both call sites (process_element1/2) only reach here once it is; the
    top-level calculation always reflects it, never a channel-specific rate."""
    assert night.blended is not None
    market = night.blended
    decided_at = datetime.now(UTC)
    # Phase 20 (ADR-0013 §5): no longer read by decide_price()'s floor math
    # (there are no antelación tiers left to select) — still recorded on the
    # event directly, informational/audit only.
    days_to_arrival = (target_date - decided_at.date()).days

    # Phase 21 (ADR-0014, spec 21 §2/§3): formalizes the top-level
    # (apartment, night) candidate this function is about to price —
    # stay_length=1, channel=None, matching decide_price()'s own defaults
    # below. Not consumed by libs/pricing-formulas (its signatures are
    # unchanged by this phase); carried purely for explainability/audit.
    # The LOS matrix and channel matrix (decide_price_los_matrix()/
    # decide_price_by_channel() below) loop internally inside
    # libs/pricing-formulas and are deliberately left untouched — no
    # separate StayCandidate is constructed per LOS/channel row (spec 21 §2's
    # design fork explains why).
    stay_candidate = StayCandidate(
        apartment_id=cost.apartment_id, arrival_date=target_date, stay_length=1
    )
    logger.debug("Pricing stay candidate: %s", stay_candidate)

    fixed_and_allocated_costs_per_night_eur = round(
        cost.fixed_cost_eur + cost.variable_cost_eur, 2
    )

    # Phase 20 (ADR-0013 §3/§4): every applicable percentage CostDefinition
    # for this apartment/period — payment-line-derived ones (Stage A) plus
    # the owner-commission one (Stage A2, kept in its own dedicated fields
    # for backward-compatible event shape). Each contributes its own rate to
    # `p` and its own netting_eur if its revenue_base excludes concepts from
    # the base — generalizing Phase 11's commission-only netting mechanism.
    all_percentage_costs = [
        *cost.percentage_costs,
        PercentageCostComponent(
            concept="owner_commission",
            rate=cost.commission_pct,
            revenue_base=cost.commission_base,
        ),
    ]
    p = round(sum(pc.rate for pc in all_percentage_costs), 4)
    netting_eur = 0.0
    netting_components = []
    for pc in all_percentage_costs:
        pc_revenue_base = cast(RevenueBase, pc.revenue_base or "total_revenue")
        net = netted_revenue_base_amount(
            pc_revenue_base,
            cost.ota_related_cost_eur,
            cost.cleaning_cost_eur,
            cost.laundry_cost_eur,
            cost.booking_scope_cost_eur,
        )
        netting_eur += round(pc.rate * net, 2)
        component = revenue_base_netting_component(pc_revenue_base, pc.rate, net)
        if component is not None:
            netting_components.append(component)
    netting_eur = round(netting_eur, 2)

    calc = decide_price(
        fixed_and_allocated_costs_per_night_eur=fixed_and_allocated_costs_per_night_eur,
        per_booking_cost_eur=cost.per_booking_cost_eur,
        p=p,
        target_margin=cost.target_margin,
        avg_nightly_rate_eur=market.avg_nightly_rate_eur,
        competitiveness_discount=cost.competitiveness_discount,
        property_attribute_factor=cost.property_attribute_factor,
        netting_eur=netting_eur,
        property_decision_components=cost.property_decision_components,
        commission_decision_components=netting_components,
        days_to_arrival=days_to_arrival,
    )
    los_matrix = decide_price_los_matrix(
        fixed_and_allocated_costs_per_night_eur=fixed_and_allocated_costs_per_night_eur,
        per_booking_cost_eur=cost.per_booking_cost_eur,
        p=p,
        target_margin=cost.target_margin,
        avg_nightly_rate_eur=market.avg_nightly_rate_eur,
        competitiveness_discount=cost.competitiveness_discount,
        property_attribute_factor=cost.property_attribute_factor,
        netting_eur=netting_eur,
        days_to_arrival=days_to_arrival,
    )
    # Phase 15 (ADR-0011 backlog #12): pure post-processing over the matrix
    # just computed above, plus the same raw cost inputs decide_price() used
    # (to compute a whole reservation's cost/price, not just its per-night
    # floor). Its own decision_component (if any) is appended to the same
    # flat top-level list Phase 11/14 share.
    minimum_stay = recommend_minimum_stay(
        los_matrix,
        calc.property_reference_price_eur,
        fixed_and_allocated_costs_per_night_eur=fixed_and_allocated_costs_per_night_eur,
        per_booking_cost_eur=cost.per_booking_cost_eur,
    )
    # Phase 16 (ADR-0011 backlog #2): one candidate per channel with an
    # observed rate for this night — [] until market-ingestor's channel
    # events for it have arrived. Never affects the top-level calculation
    # above, which always reads night.blended only. p_other excludes
    # commission (each channel uses its own fixed commission constant
    # instead, ADR-0013 leaves the channel gross-up mechanism itself
    # unchanged — spec 20 §2).
    p_other = round(sum(pc.rate for pc in cost.percentage_costs), 4)
    channel_candidates = decide_price_by_channel(
        night.channel_rates_eur(),
        fixed_and_allocated_costs_per_night_eur=fixed_and_allocated_costs_per_night_eur,
        per_booking_cost_eur=cost.per_booking_cost_eur,
        target_margin=cost.target_margin,
        competitiveness_discount=cost.competitiveness_discount,
        property_attribute_factor=cost.property_attribute_factor,
        p_other=p_other,
        revenue_base=cost.commission_base,
        ota_related_cost_eur=cost.ota_related_cost_eur,
        cleaning_cost_eur=cost.cleaning_cost_eur,
        laundry_cost_eur=cost.laundry_cost_eur,
        booking_scope_cost_eur=cost.booking_scope_cost_eur,
        days_to_arrival=days_to_arrival,
    )
    # Phase 23 (ADR-0016 §2/§4, spec 23 §7): the streak is updated with
    # THIS decision's own rule_applied first, then floor_breach_days/
    # viability_status are both derived from that already-updated streak —
    # not the pre-decision one. Doing it the other way around (classify
    # using the streak as it stood before this decision) would report
    # "persistent_floor_breach" for the very decision that just cleared the
    # floor, one decision later than a human reading the dashboard would
    # expect (spec 23 §6 AC-03's "a single intervening market_competitive
    # day resets the streak" means resets visibly in that same decision).
    # manual_override_active is always False here — Stage C
    # (stage_manual_override_enrichment.py) runs strictly after Stage B and
    # overwrites viability_status to "override_active" directly when it
    # actually applies one.
    if calc.rule_applied != "market_competitive":
        new_floor_breach_since = floor_breach_since or decided_at.date()
    else:
        new_floor_breach_since = None
    floor_breach_days = (
        (decided_at.date() - new_floor_breach_since).days
        if new_floor_breach_since
        else 0
    )
    viability_status = classify_viability(
        rule_applied=calc.rule_applied,
        below_market_by=calc.below_market_by,
        recommended_min_stay=minimum_stay.recommended_min_stay,
        any_channel_market_competitive=any(
            c.rule_applied == "market_competitive" for c in channel_candidates
        ),
        manual_override_active=False,
        floor_breach_days=floor_breach_days,
    )

    decision_components = [
        DecisionComponent(code=c.code, label=c.label, impact=c.impact)
        for c in calc.decision_components
    ]
    if minimum_stay.decision_component is not None:
        decision_components.append(
            DecisionComponent(
                code=minimum_stay.decision_component.code,
                label=minimum_stay.decision_component.label,
                impact=minimum_stay.decision_component.impact,
            )
        )
    return PriceDecision(
        decision_id=uuid4(),
        apartment_id=cost.apartment_id,
        apartment_reference=cost.apartment_reference,
        target_date=target_date,
        decided_at=decided_at,
        cost_inputs=CostInputs(
            billing_period=BillingPeriod(
                start=cost.billing_period_start, end=cost.billing_period_end
            ),
            total_monthly_cost_eur=cost.total_monthly_cost_eur,
            available_days=cost.available_days,
            fixed_cost_eur=cost.fixed_cost_eur,
            variable_cost_eur=cost.variable_cost_eur,
            fixed_and_allocated_costs_eur=fixed_and_allocated_costs_per_night_eur,
            per_booking_cost_eur=cost.per_booking_cost_eur,
            p=p,
            cost_lines_count=cost.cost_lines_count,
            # Phase 17 (ADR-0011 backlog #3), extended by Phase 20 (ADR-0013,
            # spec 20 §2) with each concept's own CostDefinition dimensions.
            cost_breakdown=[
                CostConceptAmount(
                    concept=c.concept,
                    amount_eur=c.amount_eur,
                    scope=c.scope,
                    behavior=c.behavior,
                    trigger=c.trigger,
                    calculation_base=c.calculation_base,
                    recurrence=c.recurrence,
                    allocation_method=c.allocation_method,
                )
                for c in cost.cost_breakdown
            ],
        ),
        market_inputs=MarketInputs(
            market_area=market.market_area,
            avg_nightly_rate_eur=market.avg_nightly_rate_eur,
            occupancy_rate=market.occupancy_rate,
            sample_size=market.sample_size,
            collected_at=market.collected_at,
            # max(0, ...): clock skew between the producer and this node could
            # otherwise yield a negative value, failing MarketInputs' ge=0.
            data_age_seconds=max(
                0, int((decided_at - market.collected_at).total_seconds())
            ),
        ),
        calculation=Calculation(
            target_margin=cost.target_margin,
            minimum_price_eur=calc.minimum_price_eur,
            break_even_revenue_eur=calc.break_even_revenue_eur,
            profitable_floor_eur=calc.profitable_floor_eur,
            floor_policy=calc.floor_policy,
            commission_pct=cost.commission_pct,
            # Phase 11 (ADR-0011 backlog #5): set directly from cost, same as
            # commission_pct above — decide_price() doesn't need to know
            # commission_base itself, only the already-netted EUR amount.
            commission_base=cost.commission_base,
            days_to_arrival=days_to_arrival,
            competitiveness_discount=cost.competitiveness_discount,
            # Phase 25 (ADR-0018 §2): the PricingStrategy version resolved
            # once, in Stage A, alongside target_margin/competitiveness_
            # discount above — the field "reproducibility" (external spec
            # §28) hinges on.
            pricing_strategy_version=cost.pricing_strategy_version,
            property_attribute_factor=calc.property_attribute_factor,
            property_reference_price_eur=calc.property_reference_price_eur,
            market_reference_price_eur=calc.market_reference_price_eur,
            rule_applied=calc.rule_applied,
            los_floor_matrix=[
                LosFloorCandidate(
                    stay_length=candidate.stay_length,
                    minimum_price_eur=candidate.minimum_price_eur,
                    floor_policy=candidate.floor_policy,
                    rule_applied=candidate.rule_applied,
                    suggested_price_eur=candidate.suggested_price_eur,
                    effective_margin=candidate.effective_margin,
                    decision_components=[
                        DecisionComponent(code=c.code, label=c.label, impact=c.impact)
                        for c in candidate.decision_components
                    ],
                )
                for candidate in los_matrix
            ],
            decision_components=decision_components,
            minimum_stay_recommendation=MinimumStayRecommendation(
                recommended_min_stay=minimum_stay.recommended_min_stay,
                floor_relief_eur=minimum_stay.floor_relief_eur,
                cost_per_reservation_eur=minimum_stay.cost_per_reservation_eur,
                suggested_price_per_reservation_eur=(
                    minimum_stay.suggested_price_per_reservation_eur
                ),
            ),
            channel_price_matrix=[
                ChannelPriceCandidate(
                    platform=c.platform,
                    avg_nightly_rate_eur=c.avg_nightly_rate_eur,
                    commission_pct=c.commission_pct,
                    market_reference_price_eur=c.market_reference_price_eur,
                    minimum_price_eur=c.minimum_price_eur,
                    floor_policy=c.floor_policy,
                    rule_applied=c.rule_applied,
                    suggested_price_eur=c.suggested_price_eur,
                    effective_margin=c.effective_margin,
                    decision_components=[
                        DecisionComponent(
                            code=dc.code, label=dc.label, impact=dc.impact
                        )
                        for dc in c.decision_components
                    ],
                )
                for c in channel_candidates
            ],
            viability_status=viability_status,
            floor_breach_days=floor_breach_days,
        ),
        output=Output(
            suggested_price_eur=calc.suggested_price_eur,
            effective_margin=calc.effective_margin,
            below_market_by=calc.below_market_by,
        ),
    ), new_floor_breach_since
