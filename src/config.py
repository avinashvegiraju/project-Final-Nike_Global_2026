from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent

RAW_DATA_DIR = Path(os.getenv("NIKE_RAW_DATA_DIR", WORKSPACE_ROOT / "Nike_global")).expanduser()
RAW_GLOBAL_CSV = Path(os.getenv("NIKE_RAW_CSV", RAW_DATA_DIR / "Global_Nike.csv")).expanduser()
RAW_COUNTRY_GLOB = os.getenv("NIKE_RAW_GLOB", str(RAW_DATA_DIR / "Nike_*.csv"))

ARTIFACT_DIR = PROJECT_ROOT / "artifacts"
EXPORT_DIR = PROJECT_ROOT / "exports"
DOCS_DIR = PROJECT_ROOT / "docs"
SQL_DIR = PROJECT_ROOT / "sql"

DB_PATH = ARTIFACT_DIR / "nike_global.duckdb"
BUILD_METADATA_PATH = ARTIFACT_DIR / "build_metadata.json"
INSIGHT_SUMMARY_PATH = DOCS_DIR / "INSIGHT_SUMMARY.md"

LOW_COVERAGE_ROW_THRESHOLD = 5_000
EXPECTED_LOW_COVERAGE_COUNTRIES = ("AU", "EG", "IN", "NZ")

PARQUET_EXPORT_TABLES = (
    "market_summary",
    "market_category_mix",
    "market_gender_mix",
    "market_sport_mix",
    "product_market_summary",
    "matched_product_price_benchmark",
    "matched_price_group_summary",
)

CSV_EXPORT_TABLES = (
    "schema_checks",
    "country_coverage_summary",
    "market_currency_coverage_summary",
    "market_summary",
)


def ensure_project_dirs() -> None:
    for path in (ARTIFACT_DIR, EXPORT_DIR, DOCS_DIR):
        path.mkdir(parents=True, exist_ok=True)
