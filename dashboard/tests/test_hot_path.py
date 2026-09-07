import pandas as pd
import pyarrow as pa
from dashboard.hot_path import (
    current_prices,
    price_status,
    query_latest_decision,
    to_display_row,
)


class FakeTable:
    """Records every query() call so tests can assert Scan is never used."""

    def __init__(self, items_by_apartment: dict[str, list[dict]]):
        self._items_by_apartment = items_by_apartment
        self.calls: list[dict] = []

    def query(self, **kwargs):
        self.calls.append(kwargs)
        apartment_id = kwargs["ExpressionAttributeValues"][":apartment_id"]
        items = self._items_by_apartment.get(apartment_id, [])
        return {"Items": items[:1]}

    def scan(self, **kwargs):
        raise AssertionError("hot_path must never call Scan")


def test_query_latest_decision_returns_item():
    item_ = {"apartment_id": "BCN-001", "target_date": "2026-09-01"}
    table = FakeTable({"BCN-001": [item_]})

    item = query_latest_decision(table, "BCN-001")

    assert item == item_


def test_query_latest_decision_returns_none_when_missing():
    table = FakeTable({})

    assert query_latest_decision(table, "BCN-999") is None


def test_query_uses_key_condition_not_scan():
    table = FakeTable({"BCN-001": [{"apartment_id": "BCN-001"}]})

    query_latest_decision(table, "BCN-001")

    assert len(table.calls) == 1
    call = table.calls[0]
    assert call["ScanIndexForward"] is False
    assert call["Limit"] == 1
    assert "KeyConditionExpression" in call


def test_current_prices_skips_apartments_with_no_decision():
    table = FakeTable({"BCN-001": [{"apartment_id": "BCN-001"}]})

    result = current_prices(table, ["BCN-001", "BCN-002"])

    assert list(result.keys()) == ["BCN-001"]


def test_to_display_row_flattens_cost_market_and_output():
    item = {
        "target_date": "2026-08-08",
        "cost_inputs": {
            "fixed_cost_eur": 15.7,
            "variable_cost_eur": 22.1,
            "one_time_cost_eur": 2.0,
        },
        "market_inputs": {"avg_nightly_rate_eur": 145.0},
        "calculation": {"rule_applied": "cost_protected", "target_margin": 0.05},
        "output": {
            "suggested_price_eur": 130.27,
            "effective_margin": 0.25,
            "below_market_by": 14.73,
        },
    }

    row = to_display_row("BCN-001", item)

    assert row == {
        "apartment_id": "BCN-001",
        "target_date": "2026-08-08",
        "total_cost_eur": 39.8,
        "avg_market_price_eur": 145.0,
        "suggested_price_eur": 130.27,
        "effective_margin": 0.25,
        "status": "Market Competitive",
        "min_stay_reco": None,
        "cost_per_reservation_eur": None,
        "suggested_price_per_reservation_eur": None,
    }


def test_to_display_row_surfaces_minimum_stay_recommendation_when_present():
    item = {
        "target_date": "2026-08-08",
        "cost_inputs": {
            "fixed_cost_eur": 0.0,
            "variable_cost_eur": 0.0,
            "one_time_cost_eur": 110.0,
        },
        "market_inputs": {"avg_nightly_rate_eur": 90.0},
        "calculation": {
            "target_margin": 0.05,
            "minimum_stay_recommendation": {
                "recommended_min_stay": 2,
                "floor_relief_eur": 68.75,
                "cost_per_reservation_eur": 110.0,
                "suggested_price_per_reservation_eur": 171.0,
            },
        },
        "output": {
            "suggested_price_eur": 137.5,
            "effective_margin": 0.25,
            "below_market_by": -47.5,
        },
    }

    row = to_display_row("BCN-001", item)

    assert row["min_stay_reco"] == 2
    assert row["cost_per_reservation_eur"] == 110.0
    assert row["suggested_price_per_reservation_eur"] == 171.0


def test_to_display_row_defaults_minimum_stay_recommendation_to_none_when_absent():
    item = {
        "target_date": "2026-08-08",
        "cost_inputs": {
            "fixed_cost_eur": 15.7,
            "variable_cost_eur": 22.1,
            "one_time_cost_eur": 2.0,
        },
        "market_inputs": {"avg_nightly_rate_eur": 145.0},
        "calculation": {"target_margin": 0.05},
        "output": {
            "suggested_price_eur": 130.27,
            "effective_margin": 0.25,
            "below_market_by": 14.73,
        },
    }

    row = to_display_row("BCN-001", item)

    assert row["min_stay_reco"] is None
    assert row["cost_per_reservation_eur"] is None
    assert row["suggested_price_per_reservation_eur"] is None


def test_display_rows_with_and_without_a_recommendation_convert_to_arrow():
    # Regression test: caught live against a real running stack — one
    # apartment's minimum_stay_recommendation was still None (job just
    # restarted, no fresh cost_protected decision yet) while another already
    # had a real int/float recommendation. Returning "—" for the missing
    # case made pandas/Arrow raise ArrowInvalid ("Could not convert '—' ...
    # tried to convert to double") the moment Streamlit rendered both rows
    # in one table — a str/float mix in the same column. None must convert
    # cleanly instead (Arrow's native null), which this exercises directly
    # via the same st.dataframe() conversion path, without needing Streamlit
    # itself.
    rows = [
        to_display_row(
            "BCN-001",
            {
                "target_date": "2026-08-08",
                "cost_inputs": {
                    "fixed_cost_eur": 0.0,
                    "variable_cost_eur": 0.0,
                    "one_time_cost_eur": 110.0,
                },
                "market_inputs": {"avg_nightly_rate_eur": 90.0},
                "calculation": {"target_margin": 0.05},
                "output": {
                    "suggested_price_eur": 137.5,
                    "effective_margin": 0.25,
                    "below_market_by": -47.5,
                },
            },
        ),
        to_display_row(
            "BCN-002",
            {
                "target_date": "2026-08-08",
                "cost_inputs": {
                    "fixed_cost_eur": 0.0,
                    "variable_cost_eur": 0.0,
                    "one_time_cost_eur": 110.0,
                },
                "market_inputs": {"avg_nightly_rate_eur": 90.0},
                "calculation": {
                    "target_margin": 0.05,
                    "minimum_stay_recommendation": {
                        "recommended_min_stay": 2,
                        "floor_relief_eur": 68.75,
                        "cost_per_reservation_eur": 110.0,
                        "suggested_price_per_reservation_eur": 171.0,
                    },
                },
                "output": {
                    "suggested_price_eur": 137.5,
                    "effective_margin": 0.25,
                    "below_market_by": -47.5,
                },
            },
        ),
    ]

    df = pd.DataFrame(rows)

    pa.Table.from_pandas(df)  # raises ArrowInvalid if the bug regresses


def test_price_status_below_cost_when_suggested_price_under_cost():
    assert price_status(90.0, 100.0, 0.05, 150.0) == "Price Below Cost"


def test_price_status_below_profit_when_covers_cost_but_not_target_margin():
    assert price_status(102.0, 100.0, 0.05, 150.0) == "Price Below Profit"


def test_price_status_above_market_when_clears_margin_but_beats_market():
    assert price_status(160.0, 100.0, 0.05, 150.0) == "Price Above Market"


def test_price_status_market_competitive_when_between_profit_floor_and_market():
    assert price_status(130.0, 100.0, 0.05, 150.0) == "Market Competitive"


def test_price_status_at_cost_boundary_is_not_below_cost():
    # Equal to cost is not "below" it — falls through to the next check.
    assert price_status(100.0, 100.0, 0.05, 150.0) == "Price Below Profit"


def test_price_status_at_profit_floor_boundary_is_not_below_profit():
    # Equal to the profit floor clears it (strict < is what fails, not <=).
    assert price_status(105.0, 100.0, 0.05, 150.0) == "Market Competitive"


def test_price_status_at_market_boundary_is_market_competitive():
    # Equal to market average is not "above" it.
    assert price_status(150.0, 100.0, 0.05, 150.0) == "Market Competitive"
