from datetime import UTC, date, datetime
from uuid import uuid4

from pricing_formulas.engine import (
    decide_price,
    decide_price_by_channel,
    decide_price_los_matrix,
    recommend_minimum_stay,
)
from pricing_formulas.layers.commercial import (
    commission_base_netting_component,
    netted_commission_amount,
)
from pyflink.common.typeinfo import Types
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import KeyedCoProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from shared_schemas.market_price import MarketPrice
from shared_schemas.price_decision import (
    BillingPeriod,
    Calculation,
    ChannelPriceCandidate,
    CostInputs,
    DecisionComponent,
    LosFloorCandidate,
    MarketInputs,
    MinimumStayRecommendation,
    Output,
    PriceDecision,
)

from flink_jobs.eviction import (
    expired_night_keys,
    is_over_capacity,
    oldest_key_by_updated_at,
)
from flink_jobs.models import CostAggregate, MarketSnapshot, NightSnapshot
from flink_jobs.staleness import is_safe_to_overwrite
from flink_jobs.watchdog import expired_keys, next_deadline_millis

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
            yield _build_price_decision(
                value, night, date.fromisoformat(target_date_str)
            )

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
            previous = existing.blended if platform is None else existing.channels.get(
                platform
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
            yield _build_price_decision(cost, night, target_date)

    def on_timer(self, timestamp: int, ctx):
        """Fires data_stale for apartments/nights whose deadline matches timestamp."""
        for apartment_id in expired_keys(
            dict(self.apartment_deadlines.items()), timestamp
        ):
            yield DATA_STALE_TAG, ("apartment", apartment_id)
        for night_key in expired_keys(dict(self.night_deadlines.items()), timestamp):
            yield DATA_STALE_TAG, ("night", night_key)


def _build_price_decision(
    cost: CostAggregate, night: NightSnapshot, target_date: date
) -> PriceDecision:
    """Applies the pricing formula and assembles a PriceDecision. Returns it.
    night.blended must already be resolved (Phase 16, ADR-0011 backlog #2) —
    both call sites (process_element1/2) only reach here once it is; the
    top-level calculation always reflects it, never a channel-specific rate."""
    assert night.blended is not None
    market = night.blended
    decided_at = datetime.now(UTC)
    days_to_arrival = (target_date - decided_at.date()).days

    # Phase 11 (ADR-0011 backlog #5): the netting amount and its EUR floor
    # adjustment don't vary with stay_length (same as property attributes,
    # spec 09 §A) — computed once here, forwarded to every decide_price()
    # call for the correct floor, but the explaining decision_component is
    # only attached to the top-level calculation (spec 11 §F/AC-06).
    net = netted_commission_amount(
        cost.commission_base, cost.ota_related_cost_eur, cost.cleaning_cost_eur
    )
    commission_netting_eur = round(cost.commission_pct * net, 2)
    commission_component = commission_base_netting_component(
        cost.commission_base, cost.commission_pct, net
    )
    commission_components = [commission_component] if commission_component else []

    calc = decide_price(
        fixed_cost_eur=cost.fixed_cost_eur,
        variable_cost_eur=cost.variable_cost_eur,
        one_time_cost_eur=cost.one_time_cost_eur,
        target_margin=cost.target_margin,
        commission_pct=cost.commission_pct,
        avg_nightly_rate_eur=market.avg_nightly_rate_eur,
        competitiveness_discount=cost.competitiveness_discount,
        days_to_arrival=days_to_arrival,
        property_attribute_factor=cost.property_attribute_factor,
        commission_netting_eur=commission_netting_eur,
        property_decision_components=cost.property_decision_components,
        commission_decision_components=commission_components,
    )
    los_matrix = decide_price_los_matrix(
        fixed_cost_eur=cost.fixed_cost_eur,
        variable_cost_eur=cost.variable_cost_eur,
        one_time_cost_eur=cost.one_time_cost_eur,
        target_margin=cost.target_margin,
        commission_pct=cost.commission_pct,
        avg_nightly_rate_eur=market.avg_nightly_rate_eur,
        competitiveness_discount=cost.competitiveness_discount,
        days_to_arrival=days_to_arrival,
        property_attribute_factor=cost.property_attribute_factor,
        commission_netting_eur=commission_netting_eur,
    )
    # Phase 15 (ADR-0011 backlog #12): pure post-processing over the matrix
    # just computed above, plus the same raw cost inputs decide_price() used
    # (to compute a whole reservation's cost/price, not just its per-night
    # floor). Its own decision_component (if any) is appended to the same
    # flat top-level list Phase 11/14 share.
    minimum_stay = recommend_minimum_stay(
        los_matrix,
        calc.property_reference_price_eur,
        fixed_cost_eur=cost.fixed_cost_eur,
        variable_cost_eur=cost.variable_cost_eur,
        one_time_cost_eur=cost.one_time_cost_eur,
    )
    # Phase 16 (ADR-0011 backlog #2): one candidate per channel with an
    # observed rate for this night — [] until market-ingestor's channel
    # events for it have arrived. Never affects the top-level calculation
    # above, which always reads night.blended only.
    channel_candidates = decide_price_by_channel(
        night.channel_rates_eur(),
        fixed_cost_eur=cost.fixed_cost_eur,
        variable_cost_eur=cost.variable_cost_eur,
        one_time_cost_eur=cost.one_time_cost_eur,
        target_margin=cost.target_margin,
        competitiveness_discount=cost.competitiveness_discount,
        days_to_arrival=days_to_arrival,
        property_attribute_factor=cost.property_attribute_factor,
        commission_base=cost.commission_base,
        ota_related_cost_eur=cost.ota_related_cost_eur,
        cleaning_cost_eur=cost.cleaning_cost_eur,
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
            one_time_cost_eur=cost.one_time_cost_eur,
            cost_lines_count=cost.cost_lines_count,
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
            floor_type=calc.floor_type,
            floor_policy=calc.floor_policy,
            commission_pct=cost.commission_pct,
            # Phase 11 (ADR-0011 backlog #5): set directly from cost, same as
            # commission_pct above — decide_price() doesn't need to know
            # commission_base itself, only the already-netted EUR amount.
            commission_base=cost.commission_base,
            days_to_arrival=days_to_arrival,
            competitiveness_discount=cost.competitiveness_discount,
            property_attribute_factor=calc.property_attribute_factor,
            property_reference_price_eur=calc.property_reference_price_eur,
            market_reference_price_eur=calc.market_reference_price_eur,
            rule_applied=calc.rule_applied,
            los_floor_matrix=[
                LosFloorCandidate(
                    stay_length=candidate.stay_length,
                    minimum_price_eur=candidate.minimum_price_eur,
                    floor_type=candidate.floor_type,
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
                    floor_type=c.floor_type,
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
        ),
        output=Output(
            suggested_price_eur=calc.suggested_price_eur,
            effective_margin=calc.effective_margin,
            below_market_by=calc.below_market_by,
        ),
    )
