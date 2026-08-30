from pricing_formulas.layers.market import market_reference_price


def test_discount_reduces_the_reference_price():
    assert market_reference_price(200.0, 0.05) == 190.0


def test_zero_discount_is_a_passthrough():
    assert market_reference_price(200.0, 0.0) == 200.0
