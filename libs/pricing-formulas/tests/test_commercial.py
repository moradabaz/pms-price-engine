from pricing_formulas.layers.commercial import (
    netted_revenue_base_amount,
    revenue_base_netting_component,
)


def test_netted_revenue_base_amount_per_base():
    assert netted_revenue_base_amount("total_revenue", 45.0, 25.0) == 0.0
    assert netted_revenue_base_amount("revenue_minus_ota", 45.0, 25.0) == 45.0
    assert (
        netted_revenue_base_amount("revenue_minus_ota_minus_cleaning", 45.0, 25.0)
        == 70.0
    )


def test_netted_revenue_base_amount_laundry_base():
    # spec 24 §5 AC-01.
    assert (
        netted_revenue_base_amount(
            "revenue_minus_ota_minus_cleaning_minus_laundry",
            ota_related_cost_eur=45.0,
            cleaning_cost_eur=25.0,
            laundry_cost_eur=10.0,
        )
        == 80.0
    )


def test_netted_revenue_base_amount_all_booking_costs_base():
    # spec 24 §5 AC-01: returns booking_scope_cost_eur directly.
    assert (
        netted_revenue_base_amount(
            "revenue_minus_all_booking_costs",
            ota_related_cost_eur=45.0,
            cleaning_cost_eur=25.0,
            laundry_cost_eur=10.0,
            booking_scope_cost_eur=200.0,
        )
        == 200.0
    )


def test_all_booking_costs_base_is_the_widest_netting_amount():
    # spec 24 §5 AC-01: revenue_minus_all_booking_costs >= every other base
    # for the same inputs, given booking_scope_cost_eur >= the sum of its
    # narrower sub-totals (the realistic case — booking scope includes OTA/
    # cleaning/laundry plus whatever else is booking-scoped).
    kwargs = dict(
        ota_related_cost_eur=45.0,
        cleaning_cost_eur=25.0,
        laundry_cost_eur=10.0,
        booking_scope_cost_eur=200.0,
    )
    bases = [
        "total_revenue",
        "revenue_minus_ota",
        "revenue_minus_ota_minus_cleaning",
        "revenue_minus_ota_minus_cleaning_minus_laundry",
    ]
    widest = netted_revenue_base_amount("revenue_minus_all_booking_costs", **kwargs)
    assert all(widest >= netted_revenue_base_amount(base, **kwargs) for base in bases)


def test_revenue_base_netting_component_absent_when_net_is_zero():
    # AC-05: no component at total_revenue, nor for a netting base whose
    # underlying amount happens to be zero this period (no noise for no
    # information gain, spec 11 §F).
    assert revenue_base_netting_component("total_revenue", 0.15, 0.0) is None
    assert revenue_base_netting_component("revenue_minus_ota", 0.15, 0.0) is None


def test_revenue_base_netting_component_present_when_net_is_positive():
    component = revenue_base_netting_component("revenue_minus_ota", 0.15, 45.0)
    assert component is not None
    assert component.code == "revenue_base_netting"
    assert component.impact == -6.75
