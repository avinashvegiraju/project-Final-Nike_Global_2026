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


st.set_page_config(page_title="Assortment And Availability", layout="wide")
assert_exports_ready()

product_df = load_product_market_summary()
filters = add_product_filters(product_df, key_prefix="assortment")
filtered = apply_product_filters(product_df, filters)

st.title("Assortment And Availability")

if filtered.empty:
    st.warning("No data matches the current filters.")
    st.stop()

market_rollup = (
    filtered.groupby(["country_code", "currency", "market_key"], as_index=False)
    .agg(
        unique_products=("product_id", "nunique"),
        avg_size_availability_rate=("size_availability_rate", "mean"),
        in_stock_product_rate=("in_stock_product_flag", "mean"),
        available_sizes=("available_sizes", "sum"),
        total_listed_sizes=("total_listed_sizes", "sum"),
    )
)
market_rollup["size_row_availability_rate"] = (
    market_rollup["available_sizes"] / market_rollup["total_listed_sizes"]
)

col1, col2 = st.columns((1.2, 1))

with col1:
    scatter = px.scatter(
        market_rollup,
        x="unique_products",
        y="avg_size_availability_rate",
        size="in_stock_product_rate",
        color="currency",
        hover_name="market_key",
        title="Market Assortment Breadth vs Average Size Availability",
        labels={
            "unique_products": "Unique products",
            "avg_size_availability_rate": "Average size availability rate",
            "in_stock_product_rate": "In-stock product rate",
        },
    )
    scatter.update_layout(height=500)
    st.plotly_chart(scatter, width="stretch")

with col2:
    category_rollup = (
        filtered.groupby("category", as_index=False)
        .agg(
            unique_products=("product_id", "nunique"),
            avg_size_availability_rate=("size_availability_rate", "mean"),
        )
        .sort_values("unique_products", ascending=False)
    )
    bar = px.bar(
        category_rollup,
        x="unique_products",
        y="category",
        color="avg_size_availability_rate",
        color_continuous_scale="Greens",
        orientation="h",
        title="Category Assortment Depth",
        labels={"unique_products": "Unique products", "category": "Category"},
    )
    bar.update_layout(height=500, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(bar, width="stretch")

heatmap_source = (
    filtered.groupby(["country_code", "category"], as_index=False)
    .agg(avg_size_availability_rate=("size_availability_rate", "mean"))
)
heatmap_pivot = heatmap_source.pivot(index="category", columns="country_code", values="avg_size_availability_rate")

if not heatmap_pivot.empty:
    heatmap = px.imshow(
        heatmap_pivot.fillna(0),
        aspect="auto",
        color_continuous_scale="Tealgrn",
        title="Category-Level Size Availability Heatmap",
        labels={"x": "Country", "y": "Category", "color": "Avg size availability"},
    )
    heatmap.update_layout(height=520)
    st.plotly_chart(heatmap, width="stretch")

dist_col1, dist_col2 = st.columns((1, 1))

with dist_col1:
    box = px.box(
        filtered,
        x="category",
        y="size_availability_rate",
        color="category",
        title="Product-Level Size Availability Distribution",
    )
    box.update_layout(height=500, showlegend=False)
    st.plotly_chart(box, width="stretch")

with dist_col2:
    table = market_rollup[["market_key", "unique_products", "avg_size_availability_rate", "size_row_availability_rate"]].copy()
    table["avg_size_availability_rate"] = format_pct(table["avg_size_availability_rate"])
    table["size_row_availability_rate"] = format_pct(table["size_row_availability_rate"])
    st.subheader("Filtered Market Availability Table")
    st.dataframe(table.sort_values("unique_products", ascending=False), width="stretch", hide_index=True)
