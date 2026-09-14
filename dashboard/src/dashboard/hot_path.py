from decimal import Decimal
from typing import Any

import pandas as pd

# Defensive display-layer fix: any record decided before the 2026-09-12
# rename ("cost_protected" -> "minimum_profitable_price") still carries the
# old string verbatim in rule_applied and the rule_cost_protected reason
# code — Iceberg/DynamoDB are never rewritten in place (the same "audit
# trail keeps the vocabulary of its time" convention every prior phase
# followed). A viewer must never see that raw legacy string again
# regardless of how old the underlying record is.
_LEGACY_TERM_FIX = {"cost_protected": "minimum_profitable_price"}


def descrub(value: str) -> str:
    """Rewrites any legacy term substring in one string value. Handles both
    the bare enum value and the "rule_"-prefixed reason code in one pass,
    since "cost_protected" is a substring of "rule_cost_protected" too.
    Returns the rewritten string, unchanged if no legacy term is present."""
    for old, new in _LEGACY_TERM_FIX.items():
        value = value.replace(old, new)
    return value


def descrub_df(df: pd.DataFrame) -> pd.DataFrame:
    """Same fix applied across every cell of a DataFrame before it's shown —
    covers rule_applied and decision_components.code wherever they show up,
    without needing to enumerate every column by name. Returns a rewritten
    copy; an empty DataFrame passes through untouched."""
    if df.empty:
        return df
    for old, new in _LEGACY_TERM_FIX.items():
        df = df.replace(old, new, regex=True)
    return df


def to_native(value: Any) -> Any:
    """Recursively converts DynamoDB's Decimal values (boto3's native numeric
    type) into int/float so pandas/Streamlit can render them anywhere in a
    nested price_decision item — not just the handful of fields
    to_display_row() picks out. An integral Decimal (e.g. cost_lines_count)
    becomes int, never a needlessly-.0 float. Returns the converted value,
    recursing through dicts and lists; any other type passes through
    unchanged."""
    if isinstance(value, Decimal):
        as_int = int(value)
        return as_int if as_int == value else float(value)
    if isinstance(value, dict):
        return {key: to_native(v) for key, v in value.items()}
    if isinstance(value, list):
        return [to_native(v) for v in value]
    return value


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
    property_reference_price_eur: float,
) -> str:
    """Classifies the suggested price for the dashboard's Status column.
    Checked in order, worst case first:
    - Price Below Cost: loses money outright.
    - Price Below Profit: covers cost but not the PM's target_margin.
    - Price Above Market: clears the target margin but prices above this
      apartment's own Property Reference Price (the raw market average
      adjusted for this apartment's Bonus/Malus attributes, Phase 8) —
      upside, not a guardrail failure. Compared against the *property*
      reference, not the raw segment-wide market average, so this agrees
      with Apartment Detail's "Below market by" metric (output.
      below_market_by, guardrails.py), which uses the same baseline —
      a premium apartment priced above the raw market average but below
      its own, higher reference no longer shows contradictory signals
      between the two views.
    - Market Competitive: clears the target margin and stays at/below the
      property reference price — the desired steady state.
    Returns the status label."""
    profit_floor_eur = total_cost_eur * (1 + target_margin)
    if suggested_price_eur < total_cost_eur:
        return "Price Below Cost"
    if suggested_price_eur < profit_floor_eur:
        return "Price Below Profit"
    if suggested_price_eur > property_reference_price_eur:
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
    # Phase 20 (ADR-0013): fixed_and_allocated_costs_eur + per_booking_cost_eur
    # replaces the old fixed_cost_eur + variable_cost_eur + one_time_cost_eur
    # sum — same total, new terms.
    total_cost_eur = float(
        cost_inputs["fixed_and_allocated_costs_eur"]
        + cost_inputs["per_booking_cost_eur"]
    )
    avg_market_price_eur = float(item["market_inputs"]["avg_nightly_rate_eur"])
    suggested_price_eur = float(item["output"]["suggested_price_eur"])
    target_margin = float(item["calculation"]["target_margin"])
    # Phase 8 (ADR-0011 backlog #6): property_reference_price_eur is
    # avg_market_price_eur adjusted by property_attribute_factor (Bonus/
    # Malus) — surfaced here so a premium property's suggested_price_eur
    # landing above avg_market_price_eur is visibly explained, not silently
    # inflated, and used (not avg_market_price_eur) as price_status()'s own
    # "above market" baseline, so the list status agrees with Apartment
    # Detail's "Below market by" metric instead of contradicting it. Both
    # fields exist on every decision since Phase 8; no None-fallback needed
    # the way minimum_stay_recommendation (Phase 15) requires below.
    property_reference_price_eur = float(
        item["calculation"]["property_reference_price_eur"]
    )
    property_attribute_factor = float(item["calculation"]["property_attribute_factor"])
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
        "property_reference_price_eur": property_reference_price_eur,
        "property_attribute_factor": property_attribute_factor,
        "suggested_price_eur": suggested_price_eur,
        "effective_margin": float(item["output"]["effective_margin"]),
        "status": price_status(
            suggested_price_eur,
            total_cost_eur,
            target_margin,
            property_reference_price_eur,
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
