from dashboard.hot_path import current_prices, query_latest_decision, to_display_row


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
        "calculation": {"rule_applied": "cost_protected"},
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
        "rule_applied": "cost_protected",
    }
