from __future__ import annotations

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.dashboard_utils import (
    add_product_filters,
    apply_product_filters,
    assert_exports_ready,
    format_pct,
    load_product_market_summary,
)


st.set_page_config(page_title="Product Explorer", layout="wide")
assert_exports_ready()

product_df = load_product_market_summary()
filters = add_product_filters(product_df, key_prefix="explorer")
filtered = apply_product_filters(product_df, filters)

st.title("Product Explorer")

search_text = st.sidebar.text_input("Search product or model", "").strip().lower()
if search_text:
    filtered = filtered[
        filtered["product_name"].str.lower().str.contains(search_text, na=False)
        | filtered["model_number"].str.lower().str.contains(search_text, na=False)
    ]

if filtered.empty:
    st.warning("No products match the current filters.")
    st.stop()

st.caption(f"Filtered product-market rows: {len(filtered):,}")

product_options = (
    filtered.assign(label=filtered["product_name"] + " | " + filtered["country_code"] + " | " + filtered["currency"])
    [["product_id", "label"]]
    .drop_duplicates()
    .sort_values("label")
)
selected_product_id = st.selectbox(
    "Select a product-market record",
    options=product_options["product_id"].tolist(),
    format_func=lambda pid: product_options.loc[product_options["product_id"] == pid, "label"].iloc[0],
)

product_detail = filtered[filtered["product_id"] == selected_product_id].copy()
all_markets_for_product = product_df[product_df["product_id"] == selected_product_id].copy()

detail_row = product_detail.iloc[0]
col1, col2, col3, col4 = st.columns(4)
col1.metric("Product", detail_row["product_name"])
col2.metric("Model", detail_row["model_number"])
col3.metric("Category", detail_row["category"])
col4.metric("Primary Sport", detail_row["primary_sport_tag"])

market_price = px.bar(
    all_markets_for_product.sort_values("current_price_local"),
    x="current_price_local",
    y="market_key",
    color="has_discount",
    orientation="h",
    title="Current Price By Market",
    labels={"current_price_local": "Current price", "market_key": "Market"},
)
market_price.update_layout(height=420)
st.plotly_chart(market_price, width="stretch")

availability_chart = px.bar(
    all_markets_for_product.sort_values("size_availability_rate", ascending=False),
    x="size_availability_rate",
    y="market_key",
    color="in_stock_product_flag",
    orientation="h",
    title="Size Availability Rate By Market",
    labels={"size_availability_rate": "Size availability rate", "market_key": "Market"},
)
availability_chart.update_layout(height=420)
st.plotly_chart(availability_chart, width="stretch")

table_cols = [
    "market_key",
    "current_price_local",
    "reference_price_local",
    "normalized_discount_pct",
    "has_discount",
    "available_sizes",
    "total_listed_sizes",
    "size_availability_rate",
    "in_stock_product_flag",
]
detail_table = all_markets_for_product[table_cols].copy()
detail_table["normalized_discount_pct"] = detail_table["normalized_discount_pct"].round(2).astype(str) + "%"
detail_table["size_availability_rate"] = format_pct(detail_table["size_availability_rate"])

st.subheader("Product-Market Detail")
st.dataframe(detail_table.sort_values("market_key"), width="stretch", hide_index=True)

explorer_table_cols = [
    "product_name",
    "model_number",
    "market_key",
    "category",
    "gender_segment_normalized",
    "primary_sport_tag",
    "current_price_local",
    "normalized_discount_pct",
    "size_availability_rate",
]
explorer_table = filtered[explorer_table_cols].copy()
explorer_table["normalized_discount_pct"] = explorer_table["normalized_discount_pct"].round(2).astype(str) + "%"
explorer_table["size_availability_rate"] = format_pct(explorer_table["size_availability_rate"])

st.subheader("Filtered Explorer Table")
st.dataframe(explorer_table, width="stretch", hide_index=True)
