from pricing_formulas.layers.booking_window import floor_policy_for


def test_floor_policy_for_all_three_floor_types():
    # AC-01 (spec 12): contribution is Hard, both structural_* tiers are Soft.
    assert floor_policy_for("structural_full_margin") == "soft"
    assert floor_policy_for("structural_reduced_margin") == "soft"
    assert floor_policy_for("contribution") == "hard"
