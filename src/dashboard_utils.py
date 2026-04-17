from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    from .config import BUILD_METADATA_PATH, EXPORT_DIR, INSIGHT_SUMMARY_PATH
except ImportError:
    from config import BUILD_METADATA_PATH, EXPORT_DIR, INSIGHT_SUMMARY_PATH


REQUIRED_EXPORTS = (
    "market_summary.parquet",
    "market_category_mix.parquet",
    "market_gender_mix.parquet",
    "market_sport_mix.parquet",
    "product_market_summary.parquet",
    "matched_product_price_benchmark.parquet",
    "matched_price_group_summary.parquet",
)


def exports_ready() -> bool:
    return all((EXPORT_DIR / name).exists() for name in REQUIRED_EXPORTS)


def assert_exports_ready() -> None:
    if exports_ready():
        return
    missing = [name for name in REQUIRED_EXPORTS if not (EXPORT_DIR / name).exists()]
    st.error(
        "Dashboard assets are missing. Run `python src/build_assets.py` and "
        "`python src/run_analysis.py` first.\n\nMissing files:\n- "
        + "\n- ".join(missing)
    )
    st.stop()


@st.cache_data(show_spinner=False)
def load_parquet(name: str) -> pd.DataFrame:
    return pd.read_parquet(EXPORT_DIR / f"{name}.parquet")


@st.cache_data(show_spinner=False)
def load_market_summary() -> pd.DataFrame:
    return load_parquet("market_summary")


@st.cache_data(show_spinner=False)
def load_market_category_mix() -> pd.DataFrame:
    return load_parquet("market_category_mix")


@st.cache_data(show_spinner=False)
def load_market_gender_mix() -> pd.DataFrame:
    return load_parquet("market_gender_mix")


@st.cache_data(show_spinner=False)
def load_market_sport_mix() -> pd.DataFrame:
    return load_parquet("market_sport_mix")


@st.cache_data(show_spinner=False)
def load_product_market_summary() -> pd.DataFrame:
    return load_parquet("product_market_summary")


@st.cache_data(show_spinner=False)
def load_matched_price_benchmark() -> pd.DataFrame:
    return load_parquet("matched_product_price_benchmark")


@st.cache_data(show_spinner=False)
def load_matched_group_summary() -> pd.DataFrame:
    return load_parquet("matched_price_group_summary")


@st.cache_data(show_spinner=False)
def load_build_metadata() -> dict[str, object]:
    if not BUILD_METADATA_PATH.exists():
        return {}
    return json.loads(BUILD_METADATA_PATH.read_text(encoding="utf-8"))


def load_insight_summary() -> str:
    if not INSIGHT_SUMMARY_PATH.exists():
        return "Run `python src/run_analysis.py` to generate the insight summary."
    return INSIGHT_SUMMARY_PATH.read_text(encoding="utf-8")


def format_pct(series: pd.Series | float) -> pd.Series | str:
    if isinstance(series, pd.Series):
        return (series * 100).round(2).astype(str) + "%"
    if pd.isna(series):
        return "NA"
    return f"{series * 100:.2f}%"


def add_common_market_filters(df: pd.DataFrame, *, key_prefix: str) -> dict[str, object]:
    st.sidebar.subheader("Filters")
    include_low_coverage = st.sidebar.checkbox(
        "Include low-coverage countries",
        value=False,
        key=f"{key_prefix}_include_low_coverage",
    )

    filtered = df if include_low_coverage else df[~df["is_low_coverage_country"]]
    country_options = sorted(filtered["country_code"].dropna().unique().tolist())
    currency_options = sorted(filtered["currency"].dropna().unique().tolist())

    countries = st.sidebar.multiselect(
        "Country code",
        country_options,
        default=[],
        key=f"{key_prefix}_countries",
    )
    currencies = st.sidebar.multiselect(
        "Currency",
        currency_options,
        default=[],
        key=f"{key_prefix}_currencies",
    )

    return {
        "include_low_coverage": include_low_coverage,
        "countries": countries,
        "currencies": currencies,
    }


def apply_market_filters(df: pd.DataFrame, filters: dict[str, object]) -> pd.DataFrame:
    result = df.copy()
    if not filters["include_low_coverage"]:
        result = result[~result["is_low_coverage_country"]]
    if filters["countries"]:
        result = result[result["country_code"].isin(filters["countries"])]
    if filters["currencies"]:
        result = result[result["currency"].isin(filters["currencies"])]
    return result


def add_product_filters(df: pd.DataFrame, *, key_prefix: str) -> dict[str, object]:
    filters = add_common_market_filters(df, key_prefix=key_prefix)
    filtered = apply_market_filters(df, filters)

    category_options = sorted(filtered["category"].dropna().unique().tolist())
    gender_options = sorted(filtered["gender_segment_normalized"].dropna().unique().tolist())
    sport_options = sorted(filtered["primary_sport_tag"].dropna().unique().tolist())

    filters["categories"] = st.sidebar.multiselect(
        "Category",
        category_options,
        default=[],
        key=f"{key_prefix}_categories",
    )
    filters["genders"] = st.sidebar.multiselect(
        "Gender segment",
        gender_options,
        default=[],
        key=f"{key_prefix}_genders",
    )
    filters["sports"] = st.sidebar.multiselect(
        "Primary sport tag",
        sport_options,
        default=[],
        key=f"{key_prefix}_sports",
    )
    return filters


def apply_product_filters(df: pd.DataFrame, filters: dict[str, object]) -> pd.DataFrame:
    result = apply_market_filters(df, filters)
    if filters.get("categories"):
        result = result[result["category"].isin(filters["categories"])]
    if filters.get("genders"):
        result = result[result["gender_segment_normalized"].isin(filters["genders"])]
    if filters.get("sports"):
        result = result[result["primary_sport_tag"].isin(filters["sports"])]
    return result
