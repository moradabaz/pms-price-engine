from collections.abc import Callable

import pytest


def _sample_new_image(decision_id: str, suggested_price_eur: float) -> dict:
    """A minimal, fully-populated price_decision.v1-shaped dict, the same
    shape deserialize_image() would produce from a real DynamoDB NewImage."""
    return {
        "decision_id": decision_id,
        "apartment_id": "BCN-001",
        "apartment_reference": "Passeig de Gracia 1",
        "target_date": "2026-09-01",
        "decided_at": "2026-08-04T10:00:00+00:00",
        "cost_inputs": {
            "billing_period": {"start": "2026-08-01", "end": "2026-08-31"},
            "total_monthly_cost_eur": 1500.0,
            "available_days": 30,
            "fixed_cost_eur": 15.7,
            "variable_cost_eur": 22.1,
            "one_time_cost_eur": 0.0,
            "cost_lines_count": 4,
        },
        "market_inputs": {
            "market_area": "barcelona|eixample|apartment|2",
            "avg_nightly_rate_eur": 145.0,
            "occupancy_rate": 0.7,
            "sample_size": 20,
            "collected_at": "2026-08-04T09:30:00+00:00",
            "data_age_seconds": 1800,
        },
        "calculation": {
            "target_margin": 0.05,
            "minimum_price_eur": 129.27,
            "floor_type": "structural_full_margin",
            "commission_pct": 0.15,
            "days_to_arrival": 28,
            "competitiveness_discount": 0.1,
            "market_reference_price_eur": 130.5,
            "rule_applied": "market_competitive",
        },
        "output": {
            "suggested_price_eur": suggested_price_eur,
            "currency": "EUR",
            "effective_margin": 0.25,
            "below_market_by": 1.23,
        },
    }


@pytest.fixture
def sample_new_image() -> Callable[[str, float], dict]:
    """Factory fixture: sample_new_image(decision_id, suggested_price_eur)."""
    return _sample_new_image
