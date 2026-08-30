from pricing_formulas.layers.performance import performance_layer


def test_always_returns_the_neutral_factor():
    # Stub (ADR-0011 backlog #7, spec 13 §E) — no real signal exists yet.
    assert performance_layer() == 1.0
