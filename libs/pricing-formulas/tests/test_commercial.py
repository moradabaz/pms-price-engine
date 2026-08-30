from pricing_formulas.layers.commercial import (
    commission_base_netting_component,
    netted_commission_amount,
)


def test_netted_commission_amount_per_base():
    assert netted_commission_amount("total_revenue", 45.0, 25.0) == 0.0
    assert netted_commission_amount("revenue_minus_ota", 45.0, 25.0) == 45.0
    assert (
        netted_commission_amount("revenue_minus_ota_minus_cleaning", 45.0, 25.0) == 70.0
    )


def test_commission_base_netting_component_absent_when_net_is_zero():
    # AC-05: no component at total_revenue, nor for a netting base whose
    # underlying amount happens to be zero this period (no noise for no
    # information gain, spec 11 §F).
    assert commission_base_netting_component("total_revenue", 0.15, 0.0) is None
    assert commission_base_netting_component("revenue_minus_ota", 0.15, 0.0) is None


def test_commission_base_netting_component_present_when_net_is_positive():
    component = commission_base_netting_component("revenue_minus_ota", 0.15, 45.0)
    assert component is not None
    assert component.code == "commission_base_netting"
    assert component.impact == -6.75
