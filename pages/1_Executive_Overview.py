from __future__ import annotations

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.dashboard_utils import (
    apply_market_filters,
    assert_exports_ready,
    format_pct,
    load_build_metadata,
    load_insight_summary,
    load_market_category_mix,
    load_market_summary,
    load_product_market_summary,
    add_common_market_filters,
)


st.set_page_config(page_title="Executive Overview", layout="wide")
assert_exports_ready()

market_df = load_market_summary()
product_df = load_product_market_summary()
metadata = load_build_metadata()

filters = add_common_market_filters(market_df, key_prefix="overview")
filtered_market = apply_market_filters(market_df, filters)
filtered_product = apply_market_filters(product_df, filters)

st.title("Executive Overview")

if filtered_market.empty:
    st.warning("No data matches the current filters.")
    st.stop()

total_countries = filtered_market["country_code"].nunique()
total_markets = filtered_market["market_key"].nunique()
unique_products = filtered_product["product_id"].nunique()
matched_products = (
    filtered_product.groupby(["product_id", "currency"])["country_code"]
    .nunique()
    .ge(2)
    .sum()
)

metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
metric_col1.metric("Countries In Scope", f"{total_countries:,}")
metric_col2.metric("Market Keys", f"{total_markets:,}")
metric_col3.metric("Unique Products", f"{unique_products:,}")
metric_col4.metric("Matched Product Groups", f"{matched_products:,}")

top_markets = filtered_market.sort_values("unique_products", ascending=False).head(15).copy()
top_markets["market_label"] = top_markets["country_code"] + "|" + top_markets["currency"]

overview_col1, overview_col2 = st.columns((1.4, 1))

with overview_col1:
    fig = px.bar(
        top_markets,
        x="unique_products",
        y="market_label",
        color="discount_product_rate",
        color_continuous_scale="Blues",
        orientation="h",
        title="Top Markets By Unique Products",
        labels={"unique_products": "Unique products", "market_label": "Market"},
    )
    fig.update_layout(height=540, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, width="stretch")

with overview_col2:
    global_category_mix = (
        filtered_product.groupby("category", as_index=False)["product_id"]
        .count()
        .rename(columns={"product_id": "product_market_count"})
    )
    pie = px.pie(
        global_category_mix,
        names="category",
        values="product_market_count",
        hole=0.45,
        title="Global Category Mix In Scope",
    )
    pie.update_layout(height=540)
    st.plotly_chart(pie, width="stretch")

scatter = px.scatter(
    filtered_market,
    x="unique_products",
    y="in_stock_product_rate",
    size="discount_product_rate",
    color="currency",
    hover_name="market_key",
    title="Assortment Breadth vs In-Stock Product Rate",
    labels={
        "unique_products": "Unique products",
        "in_stock_product_rate": "In-stock product rate",
        "discount_product_rate": "Discount prevalence",
    },
)
scatter.update_layout(height=480)
st.plotly_chart(scatter, width="stretch")

warning_col, summary_col = st.columns((1, 1.4))

with warning_col:
    st.subheader("Data Quality Warnings")
    low_coverage = metadata.get("detected_low_coverage_countries", [])
    if low_coverage:
        st.markdown("- Low-coverage countries: " + ", ".join(low_coverage))
    sparse_market_keys = filtered_market.loc[
        filtered_market["is_sparse_market_currency"], ["market_key", "market_currency_size_rows"]
    ].sort_values("market_currency_size_rows")
    st.dataframe(
        sparse_market_keys.rename(columns={"market_currency_size_rows": "size_rows"}),
        width="stretch",
        hide_index=True,
    )

with summary_col:
    st.subheader("Market Summary")
    display_cols = [
        "market_key",
        "unique_products",
        "unique_models",
        "in_stock_product_rate",
        "avg_size_availability_rate",
        "discount_product_rate",
        "avg_discount_pct_among_discounted",
    ]
    summary_table = filtered_market[display_cols].copy()
    for col in [
        "in_stock_product_rate",
        "avg_size_availability_rate",
        "discount_product_rate",
        "avg_discount_pct_among_discounted",
    ]:
        summary_table[col] = format_pct(summary_table[col])
    st.dataframe(summary_table.sort_values("unique_products", ascending=False), width="stretch", hide_index=True)

with st.expander("Generated Insight Summary", expanded=False):
    st.markdown(load_insight_summary())
