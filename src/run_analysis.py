from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
from scipy.stats import chi2_contingency, kruskal, mannwhitneyu

try:
    from .config import DB_PATH, EXPORT_DIR, INSIGHT_SUMMARY_PATH, ensure_project_dirs
except ImportError:
    from config import DB_PATH, EXPORT_DIR, INSIGHT_SUMMARY_PATH, ensure_project_dirs


def load_table(con: duckdb.DuckDBPyConnection, table_name: str) -> pd.DataFrame:
    return con.execute(f"SELECT * FROM {table_name}").df()


def format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "NA"
    return f"{value * 100:.2f}%" if value <= 1 else f"{value:.2f}%"


def save_csv(df: pd.DataFrame, name: str) -> Path:
    path = EXPORT_DIR / name
    df.to_csv(path, index=False)
    return path


def run_category_stock_test(product_df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]]:
    scope = product_df[
        (~product_df["is_low_coverage_country"])
        & (product_df["category"].isin(["FOOTWEAR", "APPAREL", "EQUIPMENT"]))
    ].copy()
    contingency = pd.crosstab(scope["category"], scope["in_stock_product_flag"])
    contingency.columns = ["out_of_stock", "in_stock"] if len(contingency.columns) == 2 else contingency.columns
    chi2, p_value, dof, _ = chi2_contingency(contingency)
    return contingency.reset_index(), {
        "chi2_stat": float(chi2),
        "p_value": float(p_value),
        "dof": float(dof),
        "sample_size": float(len(scope)),
    }


def run_gender_discount_test(product_df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]]:
    scope = product_df[
        (~product_df["is_low_coverage_country"])
        & (~product_df["gender_segment_normalized"].isin(["UNKNOWN", "OTHER"]))
    ].copy()
    counts = scope["gender_segment_normalized"].value_counts()
    keep = counts[counts >= 100].index
    scope = scope[scope["gender_segment_normalized"].isin(keep)]
    contingency = pd.crosstab(scope["gender_segment_normalized"], scope["has_discount"])
    contingency.columns = ["no_discount", "has_discount"] if len(contingency.columns) == 2 else contingency.columns
    chi2, p_value, dof, _ = chi2_contingency(contingency)
    return contingency.reset_index(), {
        "chi2_stat": float(chi2),
        "p_value": float(p_value),
        "dof": float(dof),
        "sample_size": float(len(scope)),
    }


def run_discount_tests(product_df: pd.DataFrame, market_df: pd.DataFrame) -> pd.DataFrame:
    currency_scope = market_df.groupby("currency")["country_code"].nunique()
    multi_market_currencies = currency_scope[currency_scope >= 2].index.tolist()

    discounted = product_df[
        (~product_df["is_low_coverage_country"])
        & (product_df["has_discount"])
        & (product_df["normalized_discount_pct"] > 0)
        & (product_df["currency"].isin(multi_market_currencies))
        & (product_df["category"].isin(["FOOTWEAR", "APPAREL", "EQUIPMENT"]))
    ].copy()

    rows: list[dict[str, object]] = []
    for currency, currency_df in discounted.groupby("currency"):
        counts = currency_df["category"].value_counts()
        eligible_categories = counts[counts >= 50].index.tolist()
        series_list = [
            currency_df.loc[currency_df["category"] == category, "normalized_discount_pct"].to_numpy()
            for category in eligible_categories
        ]

        if len(eligible_categories) >= 3:
            stat, p_value = kruskal(*series_list)
            test_name = "kruskal"
        elif len(eligible_categories) == 2:
            stat, p_value = mannwhitneyu(series_list[0], series_list[1], alternative="two-sided")
            test_name = "mannwhitney"
        else:
            continue

        rows.append(
            {
                "currency": currency,
                "test_name": test_name,
                "categories_compared": ", ".join(eligible_categories),
                "group_count": len(eligible_categories),
                "sample_size": int(len(currency_df)),
                "statistic": float(stat),
                "p_value": float(p_value),
            }
        )

    return pd.DataFrame(rows).sort_values(["p_value", "sample_size"], ascending=[True, False])


def build_manual_price_audit(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(
        """
        SELECT
            product_name,
            model_number,
            category,
            currency,
            comparable_markets,
            min_price_local,
            median_price_local,
            max_price_local,
            absolute_price_spread_local,
            spread_vs_min_pct,
            compared_countries
        FROM matched_price_group_summary
        ORDER BY spread_vs_min_pct DESC NULLS LAST, absolute_price_spread_local DESC
        LIMIT 10
        """
    ).df()


def write_insight_summary(
    metadata_df: pd.DataFrame,
    coverage_df: pd.DataFrame,
    market_df: pd.DataFrame,
    matched_groups_df: pd.DataFrame,
    category_stats: dict[str, float],
    gender_stats: dict[str, float],
    discount_tests_df: pd.DataFrame,
    audit_df: pd.DataFrame,
) -> None:
    peer_markets = market_df[~market_df["is_low_coverage_country"]].copy()
    top_assortment = peer_markets.sort_values("unique_products", ascending=False).head(5)
    top_stock = peer_markets.sort_values("in_stock_product_rate", ascending=False).head(5)
    top_discount = peer_markets.sort_values("discount_product_rate", ascending=False).head(5)
    low_coverage = coverage_df.loc[coverage_df["is_low_coverage_country"], "country_code"].tolist()

    snapshot_row = metadata_df.loc[metadata_df["check_name"] == "snapshot_date_value"].iloc[0]
    matched_count = int(len(matched_groups_df))
    currencies_with_tests = discount_tests_df["currency"].tolist() if not discount_tests_df.empty else []

    lines = [
        "# Nike Global Catalogue 2026 Insight Summary",
        "",
        "## Executive Summary",
        f"- Snapshot date validated as **{snapshot_row['actual_value']}**.",
        f"- Peer benchmarking excludes low-coverage countries by default: **{', '.join(low_coverage)}**.",
        f"- Same-currency price benchmarking produced **{matched_count:,}** matched product groups.",
        "",
        "## Top Assortment Markets",
    ]

    for row in top_assortment.itertuples(index=False):
        lines.append(
            f"- `{row.country_code}|{row.currency}`: {int(row.unique_products):,} unique products, "
            f"{format_pct(row.in_stock_product_rate)} in-stock product rate, "
            f"{format_pct(row.discount_product_rate)} discount prevalence."
        )

    lines.extend(
        [
            "",
            "## Stock Health Leaders",
        ]
    )
    for row in top_stock.itertuples(index=False):
        lines.append(
            f"- `{row.country_code}|{row.currency}`: {format_pct(row.in_stock_product_rate)} in-stock product rate "
            f"and {format_pct(row.avg_size_availability_rate)} average size availability."
        )

    lines.extend(
        [
            "",
            "## Discounting Leaders",
        ]
    )
    for row in top_discount.itertuples(index=False):
        lines.append(
            f"- `{row.country_code}|{row.currency}`: {format_pct(row.discount_product_rate)} of products discounted; "
            f"average discount among discounted products = {format_pct(row.avg_discount_pct_among_discounted)}."
        )

    lines.extend(
        [
            "",
            "## Statistical Tests",
            f"- Category vs in-stock status: chi-square = {category_stats['chi2_stat']:.2f}, "
            f"p-value = {category_stats['p_value']:.6f}, sample size = {int(category_stats['sample_size']):,}.",
            f"- Gender segment vs discount flag: chi-square = {gender_stats['chi2_stat']:.2f}, "
            f"p-value = {gender_stats['p_value']:.6f}, sample size = {int(gender_stats['sample_size']):,}.",
        ]
    )

    if discount_tests_df.empty:
        lines.append("- No currency groups met the threshold for category-level nonparametric discount tests.")
    else:
        lines.append(
            f"- Category discount depth tests ran for currencies: **{', '.join(currencies_with_tests)}**."
        )
        for row in discount_tests_df.itertuples(index=False):
            lines.append(
                f"- `{row.currency}` {row.test_name}: categories = {row.categories_compared}, "
                f"statistic = {row.statistic:.2f}, p-value = {row.p_value:.6f}, sample size = {row.sample_size:,}."
            )

    lines.extend(
        [
            "",
            "## Manual Price Audit",
            "Top matched products by relative same-currency price spread:",
        ]
    )
    for row in audit_df.itertuples(index=False):
        lines.append(
            f"- `{row.product_name}` ({row.currency}): min = {row.min_price_local:.2f}, "
            f"median = {row.median_price_local:.2f}, max = {row.max_price_local:.2f}, "
            f"spread vs min = {row.spread_vs_min_pct:.2f}%, countries = {row.compared_countries}."
        )

    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "- The project benchmarks storefront positioning, not demand or sell-through.",
            "- Same-currency comparison avoids unsupported FX assumptions.",
            "- Sparse countries remain visible in the dashboard with warnings instead of being dropped entirely.",
        ]
    )

    INSIGHT_SUMMARY_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ensure_project_dirs()
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB database not found: {DB_PATH}. Run `python src/build_assets.py` first."
        )

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        metadata_df = load_table(con, "schema_checks")
        coverage_df = load_table(con, "country_coverage_summary")
        market_df = load_table(con, "market_summary")
        product_df = load_table(con, "product_market_summary")
        matched_groups_df = load_table(con, "matched_price_group_summary")

        category_contingency, category_stats = run_category_stock_test(product_df)
        gender_contingency, gender_stats = run_gender_discount_test(product_df)
        discount_tests_df = run_discount_tests(product_df, market_df)
        audit_df = build_manual_price_audit(con)
    finally:
        con.close()

    save_csv(category_contingency, "stat_category_vs_stock_contingency.csv")
    save_csv(gender_contingency, "stat_gender_vs_discount_contingency.csv")
    save_csv(discount_tests_df, "stat_discount_tests_by_currency.csv")
    save_csv(audit_df, "manual_price_audit_top10.csv")

    write_insight_summary(
        metadata_df=metadata_df,
        coverage_df=coverage_df,
        market_df=market_df,
        matched_groups_df=matched_groups_df,
        category_stats=category_stats,
        gender_stats=gender_stats,
        discount_tests_df=discount_tests_df,
        audit_df=audit_df,
    )

    print("Analysis completed.")
    print(f"Insight summary: {INSIGHT_SUMMARY_PATH}")
    print(f"Analysis CSV exports: {EXPORT_DIR}")


if __name__ == "__main__":
    main()
