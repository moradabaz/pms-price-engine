from dataclasses import dataclass
from typing import Literal

# Phase 10 (ADR-0011 backlog #4, docs/phase-10-decision-components-design-decisions.md
# §C): one shared shape for both property-attribute and rule-decision reason
# codes, so structural.py and guardrails.py don't each need their own
# near-identical (code, label, impact) dataclass.
PropertyReasonCode = Literal[
    "property_quality_tier", "property_rating", "property_view", "property_parking"
]
RuleReasonCode = Literal[
    "rule_market_competitive", "rule_minimum_floor", "rule_cost_protected"
]
# Phase 11 (ADR-0011 backlog #5, spec 11 §4): emitted only when a
# non-total_revenue commission_base nets a non-zero amount.
CommissionReasonCode = Literal["commission_base_netting"]
# Phase 15 (ADR-0011 backlog #12, spec 15 §4): emitted by
# recommend_minimum_stay() — "recommended" when some LOS candidate clears
# cost_protected, "not_viable" when none of them do.
MinimumStayReasonCode = Literal["minimum_stay_recommended", "minimum_stay_not_viable"]
ReasonCode = (
    PropertyReasonCode | RuleReasonCode | CommissionReasonCode | MinimumStayReasonCode
)


@dataclass(frozen=True)
class DecisionComponent:
    """One structured reason code explaining part of a pricing decision.
    impact's unit is contextual to code's family: a signed adjustment
    fraction for property_* codes, a signed EUR gap for rule_*/commission_*
    codes (spec 10 §3, spec 11 §4)."""

    code: ReasonCode
    label: str
    impact: float
