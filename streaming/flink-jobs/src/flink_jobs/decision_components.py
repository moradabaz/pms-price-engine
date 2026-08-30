from dataclasses import dataclass
from typing import Literal

# Phase 10 (ADR-0011 backlog #4, docs/phase-10-decision-components-design-decisions.md
# §C): one shared shape for both property-attribute and rule-decision reason
# codes, so property_attributes.py and pricing.py don't each need their own
# near-identical (code, label, impact) dataclass.
PropertyReasonCode = Literal[
    "property_quality_tier", "property_rating", "property_view", "property_parking"
]
RuleReasonCode = Literal[
    "rule_market_competitive", "rule_minimum_floor", "rule_cost_protected"
]
ReasonCode = PropertyReasonCode | RuleReasonCode


@dataclass(frozen=True)
class DecisionComponent:
    """One structured reason code explaining part of a pricing decision.
    impact's unit is contextual to code's family: a signed adjustment
    fraction for property_* codes, a signed EUR gap for rule_* codes
    (spec 10 §3)."""

    code: ReasonCode
    label: str
    impact: float
