from typing import Any


def query_latest_decision(table: Any, apartment_id: str) -> dict[str, Any] | None:
    """Latest decision for one apartment via Query, never Scan. Returns None
    if it has no decision yet."""
    response = table.query(
        KeyConditionExpression="apartment_id = :apartment_id",
        ExpressionAttributeValues={":apartment_id": apartment_id},
        ScanIndexForward=False,
        Limit=1,
    )
    items = response.get("Items", [])
    return items[0] if items else None


def current_prices(table: Any, apartment_ids: list[str]) -> dict[str, dict[str, Any]]:
    """One Query per known apartment_id. Skips apartments with no decision
    yet instead of raising."""
    return {
        apartment_id: item
        for apartment_id in apartment_ids
        if (item := query_latest_decision(table, apartment_id)) is not None
    }


def to_display_row(apartment_id: str, item: dict[str, Any]) -> dict[str, Any]:
    """Flattens one price_decision item for the current-price table. Cost and
    margin/vs-market figures are values Flink already computed (output.*) or
    the schema's own documented total_cost_eur sum — never re-derived here.
    DynamoDB numbers arrive as Decimal; cast to float since that's all a
    display table needs and Arrow (Streamlit's dataframe renderer) doesn't
    handle Decimal natively."""
    cost_inputs = item["cost_inputs"]
    total_cost_eur = float(
        cost_inputs["fixed_cost_eur"]
        + cost_inputs["variable_cost_eur"]
        + cost_inputs["one_time_cost_eur"]
    )
    return {
        "apartment_id": apartment_id,
        "target_date": item["target_date"],
        "total_cost_eur": total_cost_eur,
        "avg_market_price_eur": float(item["market_inputs"]["avg_nightly_rate_eur"]),
        "suggested_price_eur": float(item["output"]["suggested_price_eur"]),
        "effective_margin": float(item["output"]["effective_margin"]),
        "rule_applied": item["calculation"]["rule_applied"],
    }
