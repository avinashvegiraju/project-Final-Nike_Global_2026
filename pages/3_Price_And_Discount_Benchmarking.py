from __future__ import annotations

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.dashboard_utils import (
    assert_exports_ready,
    format_pct,
    load_market_summary,
    load_matched_group_summary,
    load_matched_price_benchmark,
)


st.set_page_config(page_title="Price And Discount Benchmarking", layout="wide")
assert_exports_ready()

market_df = load_market_summary()
benchmark_df = load_matched_price_benchmark()
group_df = load_matched_group_summary()

st.title("Price And Discount Benchmarking")

currency_options = sorted(benchmark_df["currency"].dropna().unique().tolist())
default_currency = "EUR" if "EUR" in currency_options else currency_options[0]

selected_currency = st.sidebar.selectbox("Currency", currency_options, index=currency_options.index(default_currency))
include_low_coverage = st.sidebar.checkbox("Include low-coverage countries", value=False)

category_options = sorted(benchmark_df.loc[benchmark_df["currency"] == selected_currency, "category"].dropna().unique().tolist())
selected_categories = st.sidebar.multiselect("Category", category_options, default=[])

filtered_benchmark = benchmark_df[benchmark_df["currency"] == selected_currency].copy()
filtered_groups = group_df[group_df["currency"] == selected_currency].copy()
filtered_market = market_df[market_df["currency"] == selected_currency].copy()

if not include_low_coverage:
    filtered_benchmark = filtered_benchmark[~filtered_benchmark["is_low_coverage_country"]]
    filtered_market = filtered_market[~filtered_market["is_low_coverage_country"]]

if selected_categories:
    filtered_benchmark = filtered_benchmark[filtered_benchmark["category"].isin(selected_categories)]
    filtered_groups = filtered_groups[filtered_groups["category"].isin(selected_categories)]

if filtered_benchmark.empty:
    st.warning("No same-currency matched products match the current filters.")
    st.stop()

currency_market_count = filtered_benchmark["country_code"].nunique()
currency_product_count = filtered_benchmark["product_id"].nunique()
avg_spread = filtered_groups["spread_vs_min_pct"].mean()
avg_discount_rate = filtered_market["discount_product_rate"].mean()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Currency", selected_currency)
col2.metric("Comparable Countries", f"{currency_market_count:,}")
col3.metric("Matched Products", f"{currency_product_count:,}")
col4.metric("Avg Product Spread vs Min", f"{avg_spread:.2f}%")

price_col1, price_col2 = st.columns((1.1, 1))

with price_col1:
    box = px.box(
        filtered_benchmark,
        x="country_code",
        y="current_price_local",
        color="country_code",
        points="outliers",
        title=f"Current Price Distribution By Country ({selected_currency})",
        labels={"current_price_local": f"Current price ({selected_currency})", "country_code": "Country"},
    )
    box.update_layout(height=520, showlegend=False)
    st.plotly_chart(box, width="stretch")

with price_col2:
    discount_table = filtered_market[["market_key", "discount_product_rate", "avg_discount_pct_among_discounted"]].copy()
    discount_table["discount_product_rate"] = format_pct(discount_table["discount_product_rate"])
    discount_table["avg_discount_pct_among_discounted"] = format_pct(discount_table["avg_discount_pct_among_discounted"])
    st.subheader("Discount Depth By Market")
    st.dataframe(discount_table.sort_values("market_key"), width="stretch", hide_index=True)

spread_top = filtered_groups.sort_values("spread_vs_min_pct", ascending=False).head(20).copy()
spread_bar = px.bar(
    spread_top,
    x="spread_vs_min_pct",
    y="product_name",
    color="comparable_markets",
    orientation="h",
    title=f"Top Relative Price Spread Products ({selected_currency})",
    labels={"spread_vs_min_pct": "Spread vs min price (%)", "product_name": "Product"},
)
spread_bar.update_layout(height=620, yaxis={"categoryorder": "total ascending"})
st.plotly_chart(spread_bar, width="stretch")

detail_cols = [
    "product_name",
    "country_code",
    "current_price_local",
    "median_price_local",
    "price_index_vs_median_pct",
    "price_premium_vs_min_pct",
]
details = filtered_benchmark[detail_cols].copy()
details["price_index_vs_median_pct"] = details["price_index_vs_median_pct"].round(2).astype(str) + "%"
details["price_premium_vs_min_pct"] = details["price_premium_vs_min_pct"].round(2).astype(str) + "%"

st.subheader("Matched Product Price Detail")
st.dataframe(
    details.sort_values(["product_name", "country_code"]),
    width="stretch",
    hide_index=True,
)

st.caption(
    f"Average discount-product rate across filtered markets: {format_pct(avg_discount_rate)}. "
    "This page compares only products that exist in 2 or more countries sharing the selected currency."
)
