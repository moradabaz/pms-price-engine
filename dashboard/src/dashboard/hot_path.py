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


def price_status(
    suggested_price_eur: float,
    total_cost_eur: float,
    target_margin: float,
    avg_market_price_eur: float,
) -> str:
    """Classifies the suggested price for the dashboard's Status column.
    Checked in order, worst case first:
    - Price Below Cost: loses money outright.
    - Price Below Profit: covers cost but not the PM's target_margin.
    - Price Above Market: clears the target margin but prices above the raw
      market average (upside, not a guardrail failure).
    - Market Competitive: clears the target margin and stays at/below the
      market average — the desired steady state.
    Returns the status label."""
    profit_floor_eur = total_cost_eur * (1 + target_margin)
    if suggested_price_eur < total_cost_eur:
        return "Price Below Cost"
    if suggested_price_eur < profit_floor_eur:
        return "Price Below Profit"
    if suggested_price_eur > avg_market_price_eur:
        return "Price Above Market"
    return "Market Competitive"


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
    avg_market_price_eur = float(item["market_inputs"]["avg_nightly_rate_eur"])
    suggested_price_eur = float(item["output"]["suggested_price_eur"])
    target_margin = float(item["calculation"]["target_margin"])
    # Phase 15 (ADR-0011 backlog #12): may be absent on a record predating
    # this phase. Kept as None (never "—") so pandas/Arrow sees a uniform
    # numeric column across rows instead of a str/float mix, which raises
    # ArrowInvalid at render time the moment one row is missing and another
    # isn't — Streamlit's NumberColumn renders None as a blank cell on its
    # own. cost_per_reservation_eur/suggested_price_per_reservation_eur are
    # the totals for a whole reservation at recommended_min_stay nights —
    # None together with it.
    recommendation = item["calculation"].get("minimum_stay_recommendation", {})
    recommended_min_stay = recommendation.get("recommended_min_stay")
    cost_per_reservation_eur = recommendation.get("cost_per_reservation_eur")
    suggested_price_per_reservation_eur = recommendation.get(
        "suggested_price_per_reservation_eur"
    )
    return {
        "apartment_id": apartment_id,
        "target_date": item["target_date"],
        "total_cost_eur": total_cost_eur,
        "avg_market_price_eur": avg_market_price_eur,
        "suggested_price_eur": suggested_price_eur,
        "effective_margin": float(item["output"]["effective_margin"]),
        "status": price_status(
            suggested_price_eur, total_cost_eur, target_margin, avg_market_price_eur
        ),
        "min_stay_reco": (
            None if recommended_min_stay is None else int(recommended_min_stay)
        ),
        "cost_per_reservation_eur": (
            None
            if cost_per_reservation_eur is None
            else float(cost_per_reservation_eur)
        ),
        "suggested_price_per_reservation_eur": (
            None
            if suggested_price_per_reservation_eur is None
            else float(suggested_price_per_reservation_eur)
        ),
    }
