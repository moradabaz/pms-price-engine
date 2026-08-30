from pricing_formulas.layers.guardrails import rule_decision_component


def test_rule_decision_component_market_competitive():
    # spec 10 §3: impact is the headroom, market reference minus floor.
    component = rule_decision_component(
        "market_competitive",
        minimum_price_eur=21.03,
        market_reference_price_eur=114.47,
        property_reference_price_eur=120.5,
    )
    assert component.code == "rule_market_competitive"
    assert component.impact == 93.44


def test_rule_decision_component_minimum_floor():
    # impact is how far the floor sits above market, while still <= property
    # reference (property_reference=200.0 > minimum_price=195.0).
    component = rule_decision_component(
        "minimum_floor",
        minimum_price_eur=195.0,
        market_reference_price_eur=190.0,
        property_reference_price_eur=200.0,
    )
    assert component.code == "rule_minimum_floor"
    assert component.impact == 5.0


def test_rule_decision_component_cost_protected():
    # impact is how far the floor exceeds the apartment's own reference.
    component = rule_decision_component(
        "cost_protected",
        minimum_price_eur=210.0,
        market_reference_price_eur=190.0,
        property_reference_price_eur=200.0,
    )
    assert component.code == "rule_cost_protected"
    assert component.impact == 10.0
