from pricing_formulas.layers.inventory import inventory_layer


def test_always_returns_the_neutral_factor():
    # Stub (ADR-0011 backlog #7, spec 13 §E) — no anchor exists yet.
    assert inventory_layer() == 1.0
