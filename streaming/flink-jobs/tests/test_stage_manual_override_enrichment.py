from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

from fakes import FakeBroadcastContext, FakeMapState, FakeReadOnlyContext
from flink_jobs.models import ManualOverrideAssignment, ManualOverrideRow
from flink_jobs.stage_manual_override_enrichment import (
    ManualOverrideEnrichmentFunction,
    apply_manual_override,
)
from shared_schemas.price_decision import (
    BillingPeriod,
    Calculation,
    CostInputs,
    DecisionComponent,
    LosFloorCandidate,
    MarketInputs,
    MinimumStayRecommendation,
    Output,
    PriceDecision,
)

_RULE_COMPONENT = [
    DecisionComponent(code="rule_minimum_floor", label="test", impact=5.0)
]
_PROPERTY_COMPONENTS = [
    DecisionComponent(code="property_quality_tier", label="test", impact=0.0),
    DecisionComponent(code="property_rating", label="test", impact=0.0),
    DecisionComponent(code="property_view", label="test", impact=0.0),
    DecisionComponent(code="property_parking", label="test", impact=0.0),
]


def _decision(apartment_id="BCN-001", target_date=date(2026, 9, 1)) -> PriceDecision:
    return PriceDecision(
        decision_id=uuid4(),
        apartment_id=apartment_id,
        apartment_reference=apartment_id,
        target_date=target_date,
        decided_at=datetime.now(UTC),
        cost_inputs=CostInputs(
            billing_period=BillingPeriod(start=date(2026, 8, 1), end=date(2026, 8, 31)),
            total_monthly_cost_eur=1180.0,
            available_days=30,
            fixed_cost_eur=70.0,
            variable_cost_eur=38.0,
            one_time_cost_eur=10.0,
        ),
        market_inputs=MarketInputs(
            market_area="Madrid/Centro",
            avg_nightly_rate_eur=150.0,
            collected_at=datetime.now(UTC),
            data_age_seconds=0,
        ),
        calculation=Calculation(
            target_margin=0.05,
            minimum_price_eur=147.5,
            floor_type="structural_full_margin",
            floor_policy="soft",
            commission_pct=0.15,
            commission_base="total_revenue",
            days_to_arrival=45,
            competitiveness_discount=0.05,
            property_attribute_factor=1.0,
            property_reference_price_eur=150.0,
            market_reference_price_eur=142.5,
            rule_applied="minimum_floor",
            los_floor_matrix=[
                LosFloorCandidate(
                    stay_length=1,
                    minimum_price_eur=147.5,
                    floor_type="structural_full_margin",
                    floor_policy="soft",
                    rule_applied="minimum_floor",
                    suggested_price_eur=147.5,
                    effective_margin=0.25,
                    decision_components=_RULE_COMPONENT,
                )
            ],
            decision_components=_PROPERTY_COMPONENTS + _RULE_COMPONENT,
            minimum_stay_recommendation=MinimumStayRecommendation(
                recommended_min_stay=1, floor_relief_eur=0.0
            ),
        ),
        output=Output(
            suggested_price_eur=147.5, effective_margin=0.25, below_market_by=2.5
        ),
    )


def _assignment(override_price_eur=130.0, valid_until=None) -> ManualOverrideAssignment:
    return ManualOverrideAssignment(
        override_price_eur=override_price_eur,
        reason="Pre-event availability push",
        authorized_by="ops@bilemon.example",
        valid_until=valid_until or (datetime.now(UTC) + timedelta(days=1)),
    )


def _make_function():
    fn = ManualOverrideEnrichmentFunction()
    broadcast_state = FakeMapState()
    return fn, broadcast_state


# --- apply_manual_override() — pure logic, no Flink dependency ---


def test_apply_manual_override_overwrites_suggested_price_and_margin():
    decision = _decision()
    result = apply_manual_override(decision, _assignment(override_price_eur=130.0))

    assert result.output.suggested_price_eur == 130.0
    # total_cost_eur = 70 + 38 + 10 = 118; 130/118 - 1 = 0.101694... -> 0.1017
    assert result.output.effective_margin == 0.1017


def test_apply_manual_override_clamps_expected_loss_at_zero_above_floor():
    decision = _decision()  # minimum_price_eur = 147.5
    result = apply_manual_override(decision, _assignment(override_price_eur=160.0))

    assert result.calculation.manual_override.expected_loss_eur == 0.0


def test_apply_manual_override_records_positive_expected_loss_below_floor():
    decision = _decision()  # minimum_price_eur = 147.5
    result = apply_manual_override(decision, _assignment(override_price_eur=130.0))

    assert result.calculation.manual_override.expected_loss_eur == 17.5


def test_apply_manual_override_appends_component_with_signed_impact():
    decision = _decision()
    result = apply_manual_override(decision, _assignment(override_price_eur=130.0))

    components = result.calculation.decision_components
    assert len(components) == len(decision.calculation.decision_components) + 1
    override_component = components[-1]
    assert override_component.code == "manual_override_applied"
    assert override_component.impact == -17.5


def test_apply_manual_override_leaves_other_fields_unchanged():
    decision = _decision()
    result = apply_manual_override(decision, _assignment(override_price_eur=130.0))

    assert result.calculation.rule_applied == decision.calculation.rule_applied
    assert result.calculation.los_floor_matrix == decision.calculation.los_floor_matrix
    assert result.output.below_market_by == decision.output.below_market_by


def test_apply_manual_override_records_authorization_details():
    decision = _decision()
    assignment = _assignment(override_price_eur=130.0)
    result = apply_manual_override(decision, assignment)

    override = result.calculation.manual_override
    assert override.reason == assignment.reason
    assert override.authorized_by == assignment.authorized_by
    assert override.valid_until == assignment.valid_until


# --- ManualOverrideEnrichmentFunction — Flink stage, fakes only ---


def test_process_element_passes_through_when_no_override_exists():
    fn, broadcast_state = _make_function()
    ctx = FakeReadOnlyContext(broadcast_state)
    decision = _decision()

    results = list(fn.process_element(decision, ctx))

    assert results == [decision]


def test_process_element_passes_through_when_override_expired():
    fn, broadcast_state = _make_function()
    fn.process_broadcast_element(
        ManualOverrideRow(
            apartment_id="BCN-001",
            target_date=date(2026, 9, 1),
            override_price_eur=130.0,
            reason="Pre-event availability push",
            authorized_by="ops@bilemon.example",
            valid_until=datetime.now(UTC) - timedelta(days=1),
        ),
        FakeBroadcastContext(broadcast_state),
    )
    ctx = FakeReadOnlyContext(broadcast_state)

    results = list(fn.process_element(_decision(), ctx))

    assert results[0].calculation.manual_override is None
    assert results[0].output.suggested_price_eur == 147.5


def test_process_element_applies_active_override():
    fn, broadcast_state = _make_function()
    fn.process_broadcast_element(
        ManualOverrideRow(
            apartment_id="BCN-001",
            target_date=date(2026, 9, 1),
            override_price_eur=130.0,
            reason="Pre-event availability push",
            authorized_by="ops@bilemon.example",
            valid_until=datetime.now(UTC) + timedelta(days=1),
        ),
        FakeBroadcastContext(broadcast_state),
    )
    ctx = FakeReadOnlyContext(broadcast_state)

    results = list(fn.process_element(_decision(), ctx))

    assert results[0].output.suggested_price_eur == 130.0
    assert results[0].calculation.manual_override is not None


def test_process_element_does_not_affect_a_different_night():
    fn, broadcast_state = _make_function()
    fn.process_broadcast_element(
        ManualOverrideRow(
            apartment_id="BCN-001",
            target_date=date(2026, 9, 1),
            override_price_eur=130.0,
            reason="Pre-event availability push",
            authorized_by="ops@bilemon.example",
            valid_until=datetime.now(UTC) + timedelta(days=1),
        ),
        FakeBroadcastContext(broadcast_state),
    )
    ctx = FakeReadOnlyContext(broadcast_state)

    other_night = _decision(target_date=date(2026, 9, 2))
    results = list(fn.process_element(other_night, ctx))

    assert results == [other_night]
