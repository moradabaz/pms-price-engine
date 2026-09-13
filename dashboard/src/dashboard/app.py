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


@st.cache_data(ttl=_SETTINGS.marts_cache_ttl_seconds)
def _channel_pricing(apartment_id: str, target_date: str) -> pd.DataFrame:
    return marts.channel_pricing(_SETTINGS, apartment_id, target_date)


@st.cache_data(ttl=_SETTINGS.marts_cache_ttl_seconds)
def _cost_breakdown(apartment_id: str) -> pd.DataFrame:
    return marts.cost_breakdown(_SETTINGS, apartment_id)


_STATUS_COLOR = {
    "Price Below Cost": "color: red",
    "Price Below Profit": "color: orange",
    "Market Competitive": "color: green",
    "Price Above Market": "color: orange",
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
        df.style.map(lambda v: _STATUS_COLOR.get(v, ""), subset=["status"])
        .map(lambda _: "color: red", subset=["total_cost_eur"])
        .map(lambda _: "color: blue", subset=["avg_market_price_eur"])
        if not df.empty
        else df
    )
    event = st.dataframe(
        table,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="current_prices_table",
        column_config={
            "apartment_id": "Apartment",
            "target_date": "Night",
            "total_cost_eur": st.column_config.NumberColumn(
                "Cost (1-night)", format="euro"
            ),
            "avg_market_price_eur": st.column_config.NumberColumn(
                "Market avg (raw)", format="euro"
            ),
            "property_attribute_factor": st.column_config.NumberColumn(
                "Bonus/Malus", format="%.2fx"
            ),
            "property_reference_price_eur": st.column_config.NumberColumn(
                "Property ref. price", format="euro"
            ),
            "suggested_price_eur": st.column_config.NumberColumn(
                "Suggested price", format="euro"
            ),
            "effective_margin": st.column_config.NumberColumn(
                "Margin", format="percent"
            ),
            "status": "Status",
            "min_stay_reco": st.column_config.NumberColumn(
                "Min. stay reco.", format="%d"
            ),
            "cost_per_reservation_eur": st.column_config.NumberColumn(
                "Cost per reservation", format="euro"
            ),
            "suggested_price_per_reservation_eur": st.column_config.NumberColumn(
                "Suggested price (reservation)", format="euro"
            ),
        },
    )
    if missing:
        st.caption(f"No decision yet for: {', '.join(missing)}")
    # Click a row to open its full trace in "Apartment detail" — only update
    # on a real selection, never clear it back to nothing on an unrelated
    # rerun (the 60s auto-refresh included), so the detail tab stays put.
    selected_rows = event["selection"]["rows"] if not df.empty else []
    if selected_rows:
        st.session_state["selected_apartment"] = df.iloc[selected_rows[0]][
            "apartment_id"
        ]


def render_price_evolution() -> None:
    st.header("Price evolution")
    st.caption(_freshness_label())
    apartment_id = st.selectbox("Apartment", _apartment_ids())
    if apartment_id:
        df = hot_path.descrub_df(_price_evolution(apartment_id))
        st.line_chart(df, x="target_date", y="suggested_price_eur")
        st.dataframe(df, hide_index=True)


def render_margin_alerts() -> None:
    st.header("Margin alerts (below minimum profitable price)")
    st.caption(_freshness_label())
    st.dataframe(_margin_alerts(), hide_index=True)


def render_channel_pricing() -> None:
    # Phase 16 (ADR-0011 backlog #2): a separate tab, not new columns on
    # "Current price" — that table is already dense after Phase 15.
    st.header("Channel pricing")
    st.caption(_freshness_label())
    apartment_id = st.selectbox(
        "Apartment", _apartment_ids(), key="channel_pricing_apartment"
    )
    if not apartment_id:
        return
    evolution = _price_evolution(apartment_id)
    if evolution.empty:
        st.caption("No decisions yet for this apartment.")
        return
    target_date = st.selectbox(
        "Night", evolution["target_date"], key="channel_pricing_night"
    )
    df = hot_path.descrub_df(_channel_pricing(apartment_id, str(target_date)))
    if df.empty:
        st.caption("No channel data for this night yet.")
        return
    st.dataframe(
        df,
        hide_index=True,
        column_config={
            "platform": "Channel",
            "avg_nightly_rate_eur": st.column_config.NumberColumn(
                "Market rate", format="euro"
            ),
            "commission_pct": st.column_config.NumberColumn(
                "Commission", format="percent"
            ),
            "market_reference_price_eur": st.column_config.NumberColumn(
                "Reference price", format="euro"
            ),
            "minimum_price_eur": st.column_config.NumberColumn(
                "Cost floor", format="euro"
            ),
            "rule_applied": "Rule",
            "suggested_price_eur": st.column_config.NumberColumn(
                "Suggested price", format="euro"
            ),
            "effective_margin": st.column_config.NumberColumn(
                "Margin", format="percent"
            ),
        },
    )


def render_cost_breakdown() -> None:
    # Phase 17 (ADR-0011 backlog #3): a separate tab, same reasoning as
    # Phase 16's channel pricing — this doesn't fit as new columns on
    # "Current price".
    st.header("Cost breakdown")
    st.caption(_freshness_label())
    apartment_id = st.selectbox(
        "Apartment", _apartment_ids(), key="cost_breakdown_apartment"
    )
    if not apartment_id:
        return
    df = _cost_breakdown(apartment_id)
    if df.empty:
        st.caption("No cost breakdown for this apartment yet.")
        return
    st.bar_chart(df, x="concept", y="amount_eur", height=480, use_container_width=True)
    st.dataframe(
        df,
        hide_index=True,
        column_config={
            "concept": st.column_config.TextColumn(
                "Concept", help="Cost category (e.g. electricity, cleaning, OTA fee)."
            ),
            "amount_eur": st.column_config.NumberColumn(
                "Cost (EUR/day)",
                format="euro",
                help=(
                    "This category's share of your daily cost for the current billing "
                    "period."
                ),
            ),
        },
    )


# Plain-language names/explanations for rule_applied, shown to a property
# manager who has never seen this codebase — kept next to the tab that
# displays it rather than in hot_path.py, since it's presentation only.
# Keyed by the CURRENT vocabulary only — hot_path.descrub() above always runs
# first, so a legacy value never reaches this lookup.
_RULE_APPLIED_LABEL = {
    "market_competitive": "Market competitive",
    "minimum_floor": "Minimum floor (still competitive)",
    "minimum_profitable_price": "Minimum profitable price (cost floor)",
}
_RULE_APPLIED_HELP = (
    "Which rule decided this price. Market competitive: priced at your "
    "property's own market reference. Minimum floor: your costs set the "
    "price, but it's still at or below what similar listings charge. "
    "Minimum profitable price: your costs are high enough that the price "
    "had to go above the raw market average to protect your margin — the "
    "only ways to lower it are cutting costs or accepting a smaller margin."
)


def render_apartment_detail() -> None:
    """One apartment's full decision trace — every field Flink computed for
    its latest decision, plus its historical price evolution. Reachable by
    clicking a row in "Current price" (sets st.session_state
    ["selected_apartment"]) or by the selectbox below directly. Every table
    carries a column-header tooltip (hover) plus a plain-language caption,
    so a property manager (not just an engineer) can read it unassisted."""
    st.header("Apartment detail")
    st.caption(_freshness_label())
    apartment_ids = _apartment_ids()
    if not apartment_ids:
        return
    preselected = st.session_state.get("selected_apartment")
    default_index = (
        apartment_ids.index(preselected) if preselected in apartment_ids else 0
    )
    apartment_id = st.selectbox(
        "Apartment", apartment_ids, index=default_index, key="detail_apartment"
    )
    if not apartment_id:
        return

    item = hot_path.query_latest_decision(_price_decision_table(), apartment_id)
    if item is None:
        st.caption("No decision yet for this apartment.")
        return
    decision = hot_path.to_native(item)
    cost_inputs = decision["cost_inputs"]
    market_inputs = decision["market_inputs"]
    calculation = decision["calculation"]
    output = decision["output"]
    total_cost_eur = (
        cost_inputs["fixed_and_allocated_costs_eur"]
        + cost_inputs["per_booking_cost_eur"]
    )
    rule_applied = hot_path.descrub(calculation["rule_applied"])

    st.subheader(
        f"{decision.get('apartment_reference', apartment_id)} — "
        f"{decision['target_date']}"
    )
    st.caption(f"Decided at {decision['decided_at']}")

    cols = st.columns(4)
    cols[0].metric(
        "Suggested price",
        f"{output['suggested_price_eur']:.2f} €",
        help="The nightly rate we recommend charging for this specific night.",
    )
    cols[1].metric(
        "Effective margin",
        f"{output['effective_margin'] * 100:.1f} %",
        help=(
            "Profit this price leaves once your cost for the night is covered, as a "
            "percentage of that cost — not of the price."
        ),
    )
    cols[2].metric(
        "Rule applied",
        _RULE_APPLIED_LABEL.get(rule_applied, rule_applied),
        help=_RULE_APPLIED_HELP,
    )
    # Sign flipped for display (below_market_by itself is positive when
    # priced below market) so delta_color="inverse" reads naturally: negative
    # (priced below market, good) shows green, positive (priced above
    # market) shows red.
    below_market_display_eur = -output["below_market_by"]
    cols[3].metric(
        "Below market by",
        f"{below_market_display_eur:.2f} €",
        delta=f"{below_market_display_eur:.2f} €",
        delta_color="inverse",
        help=(
            "How far the suggested price sits from your property's own market "
            "reference price. Negative (green) means priced below it. Positive "
            "(red) means priced above it (expected when the rule applied is "
            "'Minimum profitable price')."
        ),
    )

    st.markdown("**Cost** — what it costs you to host one night here.")
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "Total cost (1-night)": total_cost_eur,
                    "Fixed (info)": cost_inputs["fixed_cost_eur"],
                    "Variable (info)": cost_inputs["variable_cost_eur"],
                    "Per-booking": cost_inputs["per_booking_cost_eur"],
                    "Lines": cost_inputs.get("cost_lines_count"),
                }
            ]
        ),
        hide_index=True,
        column_config={
            "Total cost (1-night)": st.column_config.NumberColumn(
                format="euro",
                help=(
                    "Fixed-and-allocated + Per-booking below — your all-in cost for "
                    "this night."
                ),
            ),
            "Fixed (info)": st.column_config.NumberColumn(
                format="euro",
                help=(
                    "Informational only — costs you pay regardless of bookings "
                    "(e.g. rent, insurance), spread across the days in this billing "
                    "period. Already counted inside Total cost above."
                ),
            ),
            "Variable (info)": st.column_config.NumberColumn(
                format="euro",
                help=(
                    "Informational only — costs that scale with bookings/usage "
                    "(e.g. utilities). Already counted inside Total cost above."
                ),
            ),
            "Per-booking": st.column_config.NumberColumn(
                format="euro",
                help=(
                    "Costs tied to a single stay (e.g. cleaning), averaged per "
                    "turnover and split across the nights of the stay."
                ),
            ),
            "Lines": st.column_config.NumberColumn(
                help="How many expense records fed into this calculation.",
            ),
        },
    )
    st.markdown(
        "**Break-Even → Profitable Floor → Suggested** — how the price floor was "
        "built up."
    )
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "Break-even (BER)": calculation["break_even_revenue_eur"],
                    "Profitable floor (MPR)": calculation["profitable_floor_eur"],
                    "Suggested price": output["suggested_price_eur"],
                    "p (% costs)": cost_inputs["p"],
                }
            ]
        ),
        hide_index=True,
        column_config={
            "Break-even (BER)": st.column_config.NumberColumn(
                format="euro",
                help=(
                    "The price that covers cost with zero margin. Informational "
                    "only — never itself the enforced floor."
                ),
            ),
            "Profitable floor (MPR)": st.column_config.NumberColumn(
                format="euro",
                help=(
                    "The price that covers cost plus your target margin — this IS "
                    "the enforced floor (minimum_price_eur)."
                ),
            ),
            "Suggested price": st.column_config.NumberColumn(format="euro"),
            "p (% costs)": st.column_config.NumberColumn(
                format="percent",
                help=(
                    "Combined rate of every percentage-based cost (OTA fee, owner "
                    "commission, etc.) charged against this price."
                ),
            ),
        },
    )
    cost_breakdown = pd.DataFrame(cost_inputs.get("cost_breakdown", []))
    if not cost_breakdown.empty:
        st.caption(
            "Same total cost above, split by category (EUR/day) so you can "
            "see what's actually driving it. Grouped by scope — a company-wide "
            "cost (e.g. shared office rent) is labeled 'company', not 'property'."
        )
        st.bar_chart(
            cost_breakdown,
            x="concept",
            y="amount_eur",
            color="scope" if "scope" in cost_breakdown.columns else None,
            height=480,
            use_container_width=True,
        )

    st.markdown("**Market** — what similar properties nearby are charging right now.")
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "Market area": market_inputs["market_area"],
                    "Market avg (raw)": market_inputs["avg_nightly_rate_eur"],
                    "Occupancy": market_inputs.get("occupancy_rate"),
                    "Sample size": market_inputs.get("sample_size"),
                    "Collected at": market_inputs["collected_at"],
                    "Data age (s)": market_inputs["data_age_seconds"],
                }
            ]
        ),
        hide_index=True,
        column_config={
            "Market area": st.column_config.TextColumn(
                help="The city/neighborhood segment used to find comparable listings.",
            ),
            "Market avg (raw)": st.column_config.NumberColumn(
                format="euro",
                help=(
                    "What comparable listings in your area charge, BEFORE any "
                    "adjustment for your specific property."
                ),
            ),
            "Occupancy": st.column_config.NumberColumn(
                format="percent",
                help="Estimated share of nights booked in this market segment.",
            ),
            "Sample size": st.column_config.NumberColumn(
                help="Number of comparable listings behind the market average above.",
            ),
            "Collected at": st.column_config.TextColumn(
                help="When this market data was last refreshed.",
            ),
            "Data age (s)": st.column_config.NumberColumn(
                help=(
                    "How old that market data is, in seconds, at the moment this price "
                    "was decided."
                ),
            ),
        },
    )

    st.markdown(
        "**Calculation** — how we go from the raw market average to your "
        "suggested price."
    )
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "Property attribute factor (Bonus/Malus)": calculation[
                        "property_attribute_factor"
                    ],
                    "Property ref. price": calculation["property_reference_price_eur"],
                    "Market ref. price": calculation["market_reference_price_eur"],
                    "Competitiveness discount": calculation["competitiveness_discount"],
                    "Cost floor": calculation["minimum_price_eur"],
                    "Floor policy": calculation["floor_policy"],
                    "Commission %": calculation["commission_pct"],
                    "Commission base": calculation["commission_base"],
                    "Days to arrival": calculation["days_to_arrival"],
                }
            ]
        ),
        hide_index=True,
        column_config={
            "Property attribute factor (Bonus/Malus)": st.column_config.NumberColumn(
                format="%.2fx",
                help=(
                    "How much more (>1.0) or less (<1.0) than the raw market average "
                    "your property is worth, based on its quality tier, rating, view, "
                    "and parking."
                ),
            ),
            "Property ref. price": st.column_config.NumberColumn(
                format="euro",
                help=(
                    "Your property's own fair price: the raw market average multiplied "
                    "by the Bonus/Malus factor above."
                ),
            ),
            "Market ref. price": st.column_config.NumberColumn(
                format="euro",
                help=(
                    "Property ref. price minus the competitiveness discount below "
                    "— the target price when the market rule applies."
                ),
            ),
            "Competitiveness discount": st.column_config.NumberColumn(
                format="percent",
                help=(
                    "How much we shave off your property's own reference price to stay "
                    "attractive against nearby competitors."
                ),
            ),
            "Cost floor": st.column_config.NumberColumn(
                format="euro",
                help=(
                    "The minimum price that guarantees you cover cost and hit your "
                    "target margin for this night — never suggested below this."
                ),
            ),
            "Floor policy": st.column_config.TextColumn(
                help=(
                    "Hard: a zero-margin floor (Break-even and Profitable floor "
                    "coincide). Soft: a target-margin floor above break-even."
                ),
            ),
            "Commission %": st.column_config.NumberColumn(
                format="percent",
                help=(
                    "The cut taken by your booking channel (OTA) or payment processor, "
                    "already factored into the cost floor."
                ),
            ),
            "Commission base": st.column_config.TextColumn(
                help=(
                    "Which revenue figure that commission percentage is charged "
                    "against."
                ),
            ),
            "Days to arrival": st.column_config.NumberColumn(
                help="How many days from today until this specific night.",
            ),
        },
    )

    st.markdown(
        "**Why this price (decision components)** — the step-by-step "
        "reasons behind the number above, one row per factor."
    )
    st.dataframe(
        hot_path.descrub_df(pd.DataFrame(calculation.get("decision_components", []))),
        hide_index=True,
        column_config={
            "code": st.column_config.TextColumn(
                "Reason", help="Which factor this row explains."
            ),
            "label": st.column_config.TextColumn(
                "Explanation", help="Plain-language detail for this specific decision."
            ),
            "impact": st.column_config.NumberColumn(
                "Impact",
                help=(
                    "The size of this factor's effect — a % adjustment for property "
                    "factors, an EUR amount for pricing-rule factors."
                ),
            ),
        },
    )

    los_matrix = hot_path.descrub_df(
        pd.DataFrame(calculation.get("los_floor_matrix", []))
    )
    if not los_matrix.empty:
        st.markdown(
            "**Stay-length matrix** — the same calculation repeated for "
            "longer stays. A one-time cost like cleaning matters less per "
            "night the longer the guest stays, so longer stays can clear "
            "the cost floor even when a single night can't."
        )
        st.dataframe(
            los_matrix[
                [
                    "stay_length",
                    "minimum_price_eur",
                    "floor_policy",
                    "rule_applied",
                    "suggested_price_eur",
                    "effective_margin",
                ]
            ],
            hide_index=True,
            column_config={
                "stay_length": st.column_config.NumberColumn(
                    "Nights", help="Length of stay this row prices for."
                ),
                "minimum_price_eur": st.column_config.NumberColumn(
                    "Cost floor", format="euro", help="Cost floor at this stay length."
                ),
                "floor_policy": st.column_config.TextColumn(
                    "Floor policy", help="Hard (zero-margin) or soft (target-margin)."
                ),
                "rule_applied": st.column_config.TextColumn(
                    "Rule", help=_RULE_APPLIED_HELP
                ),
                "suggested_price_eur": st.column_config.NumberColumn(
                    "Suggested price/night",
                    format="euro",
                    help=(
                        "What we'd suggest per night for a reservation of this length."
                    ),
                ),
                "effective_margin": st.column_config.NumberColumn(
                    "Margin",
                    format="percent",
                    help="Profit margin at this stay length.",
                ),
            },
        )

    channel_matrix = hot_path.descrub_df(
        pd.DataFrame(calculation.get("channel_price_matrix", []))
    )
    if not channel_matrix.empty:
        st.markdown(
            "**Channel pricing** — what we'd suggest if this night were "
            "priced separately per booking channel, each with its own "
            "commission and market rate."
        )
        st.dataframe(
            channel_matrix[
                [
                    "platform",
                    "avg_nightly_rate_eur",
                    "commission_pct",
                    "suggested_price_eur",
                    "effective_margin",
                    "rule_applied",
                ]
            ],
            hide_index=True,
            column_config={
                "platform": st.column_config.TextColumn(
                    "Channel", help="Airbnb, Booking.com, or Vrbo."
                ),
                "avg_nightly_rate_eur": st.column_config.NumberColumn(
                    "Market rate",
                    format="euro",
                    help="What comparable listings charge on this specific channel.",
                ),
                "commission_pct": st.column_config.NumberColumn(
                    "Commission",
                    format="percent",
                    help="This channel's own commission cut.",
                ),
                "suggested_price_eur": st.column_config.NumberColumn(
                    "Suggested price",
                    format="euro",
                    help="What we'd suggest charging on this channel specifically.",
                ),
                "effective_margin": st.column_config.NumberColumn(
                    "Margin", format="percent", help="Profit margin on this channel."
                ),
                "rule_applied": st.column_config.TextColumn(
                    "Rule", help=_RULE_APPLIED_HELP
                ),
            },
        )

    recommendation = calculation.get("minimum_stay_recommendation") or {}
    if recommendation.get("recommended_min_stay") is not None:
        st.markdown(
            "**Minimum stay recommendation** — if a single night isn't "
            "profitable on its own, the shortest stay that would be, and "
            "what to charge for the whole reservation."
        )
        st.dataframe(
            pd.DataFrame([recommendation]),
            hide_index=True,
            column_config={
                "recommended_min_stay": st.column_config.NumberColumn(
                    "Min. nights to require",
                    help=(
                        "Shortest stay length at which this night stops being priced "
                        "below profitability."
                    ),
                ),
                "floor_relief_eur": st.column_config.NumberColumn(
                    "Relief vs. 1 night",
                    format="euro",
                    help=(
                        "How much lower the cost floor is at this stay length compared "
                        "to a single night."
                    ),
                ),
                "cost_per_reservation_eur": st.column_config.NumberColumn(
                    "Cost (whole stay)",
                    format="euro",
                    help=(
                        "Total cost for the whole reservation at the recommended "
                        "length."
                    ),
                ),
                "suggested_price_per_reservation_eur": st.column_config.NumberColumn(
                    "Suggested price (whole stay)",
                    format="euro",
                    help=(
                        "What to charge in total for the whole reservation at the "
                        "recommended length."
                    ),
                ),
            },
        )

    manual_override = calculation.get("manual_override")
    if manual_override is not None:
        st.markdown(
            "**Active manual override** — a person has manually set this "
            "night's price, overriding the algorithm's own suggestion above."
        )
        st.dataframe(
            pd.DataFrame([manual_override]),
            hide_index=True,
            column_config={
                "override_price_eur": st.column_config.NumberColumn(
                    "Override price",
                    format="euro",
                    help="The price a person set manually.",
                ),
                "reason": st.column_config.TextColumn(
                    "Reason", help="Why this override was authorized."
                ),
                "authorized_by": st.column_config.TextColumn(
                    "Authorized by", help="Who authorized this override."
                ),
                "valid_until": st.column_config.TextColumn(
                    "Valid until",
                    help=(
                        "When this override expires and the algorithm resumes control."
                    ),
                ),
                "expected_loss_eur": st.column_config.NumberColumn(
                    "Expected loss vs. floor",
                    format="euro",
                    help=(
                        "How much below the algorithmic cost floor this override sits, "
                        "if at all."
                    ),
                ),
            },
        )

    st.markdown(
        "**History** — how the suggested price for this apartment has "
        "moved across nights."
    )
    history = hot_path.descrub_df(_price_evolution(apartment_id))
    if history.empty:
        st.caption("No history yet for this apartment.")
    else:
        st.line_chart(history, x="target_date", y="suggested_price_eur")
        st.dataframe(
            history,
            hide_index=True,
            column_config={
                "target_date": st.column_config.TextColumn("Night"),
                "suggested_price_eur": st.column_config.NumberColumn(
                    "Suggested price", format="euro"
                ),
                "rule_applied": st.column_config.TextColumn(
                    "Rule", help=_RULE_APPLIED_HELP
                ),
                "floor_policy": st.column_config.TextColumn("Floor policy"),
                "effective_margin": st.column_config.NumberColumn(
                    "Margin", format="percent"
                ),
            },
        )


@st.fragment(run_every="60s")
def render_dashboard() -> None:
    (
        tab_current,
        tab_detail,
        tab_evolution,
        tab_alerts,
        tab_channels,
        tab_costs,
    ) = st.tabs(
        [
            "Current price",
            "Apartment detail",
            "Price evolution",
            "Margin alerts",
            "Channel pricing",
            "Cost breakdown",
        ]
    )
    with tab_current:
        render_current_prices()
    with tab_detail:
        render_apartment_detail()
    with tab_evolution:
        render_price_evolution()
    with tab_alerts:
        render_margin_alerts()
    with tab_channels:
        render_channel_pricing()
    with tab_costs:
        render_cost_breakdown()


def main() -> None:
    configure_logging(_SETTINGS.log_level)
    st.set_page_config(page_title="PMS Price Engine", layout="wide")
    render_dashboard()


if __name__ == "__main__":
    main()
