from typing import Any

import boto3
import pandas as pd
import streamlit as st
from common import configure_logging

from dashboard import hot_path, marts
from dashboard.settings import DashboardSettings

_SETTINGS = DashboardSettings()


@st.cache_resource
def _price_decision_table() -> Any:
    resource = boto3.resource(
        "dynamodb",
        region_name=_SETTINGS.aws_region,
        endpoint_url=_SETTINGS.dynamodb_endpoint_url,
    )
    return resource.Table(_SETTINGS.price_decision_table_name)


@st.cache_data(ttl=_SETTINGS.marts_cache_ttl_seconds)
def _apartment_ids() -> list[str]:
    return marts.list_apartment_ids(_SETTINGS)


@st.cache_data(ttl=_SETTINGS.marts_cache_ttl_seconds)
def _freshness_label() -> str:
    when = marts.freshness(_SETTINGS)
    return f"Cold path last updated: {when}" if when else "Cold path: no data yet"


@st.cache_data(ttl=_SETTINGS.marts_cache_ttl_seconds)
def _price_evolution(apartment_id: str) -> pd.DataFrame:
    return marts.price_evolution(_SETTINGS, apartment_id)


@st.cache_data(ttl=_SETTINGS.marts_cache_ttl_seconds)
def _margin_alerts() -> pd.DataFrame:
    return marts.margin_alerts(_SETTINGS)


_RULE_COLOR = {
    "cost_protected": "color: red",
    "market_competitive": "color: green",
}


def render_current_prices() -> None:
    st.header("Current price per apartment")
    apartment_ids = _apartment_ids()
    prices = hot_path.current_prices(_price_decision_table(), apartment_ids)
    rows = [
        hot_path.to_display_row(apartment_id, item)
        for apartment_id, item in prices.items()
    ]
    missing = [a for a in apartment_ids if a not in prices]
    df = pd.DataFrame(rows)
    table = (
        df.style.map(lambda v: _RULE_COLOR.get(v, ""), subset=["rule_applied"])
        if not df.empty
        else df
    )
    st.dataframe(
        table,
        hide_index=True,
        column_config={
            "apartment_id": "Apartment",
            "target_date": "Night",
            "total_cost_eur": st.column_config.NumberColumn("Cost", format="euro"),
            "avg_market_price_eur": st.column_config.NumberColumn(
                "Market avg", format="euro"
            ),
            "suggested_price_eur": st.column_config.NumberColumn(
                "Suggested price", format="euro"
            ),
            "effective_margin": st.column_config.NumberColumn(
                "Margin vs cost", format="percent"
            ),
            "rule_applied": "Rule",
        },
    )
    if missing:
        st.caption(f"No decision yet for: {', '.join(missing)}")


def render_price_evolution() -> None:
    st.header("Price evolution")
    st.caption(_freshness_label())
    apartment_id = st.selectbox("Apartment", _apartment_ids())
    if apartment_id:
        df = _price_evolution(apartment_id)
        st.line_chart(df, x="target_date", y="suggested_price_eur")
        st.dataframe(df, hide_index=True)


def render_margin_alerts() -> None:
    st.header("Margin alerts (cost_protected)")
    st.caption(_freshness_label())
    st.dataframe(_margin_alerts(), hide_index=True)


@st.fragment(run_every="60s")
def render_dashboard() -> None:
    tab_current, tab_evolution, tab_alerts = st.tabs(
        ["Current price", "Price evolution", "Margin alerts"]
    )
    with tab_current:
        render_current_prices()
    with tab_evolution:
        render_price_evolution()
    with tab_alerts:
        render_margin_alerts()


def main() -> None:
    configure_logging(_SETTINGS.log_level)
    st.set_page_config(page_title="PMS Price Engine", layout="wide")
    render_dashboard()


if __name__ == "__main__":
    main()
