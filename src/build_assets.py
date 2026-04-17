from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

try:
    from .config import (
        BUILD_METADATA_PATH,
        CSV_EXPORT_TABLES,
        DB_PATH,
        EXPECTED_LOW_COVERAGE_COUNTRIES,
        EXPORT_DIR,
    LOW_COVERAGE_ROW_THRESHOLD,
    PARQUET_EXPORT_TABLES,
    RAW_COUNTRY_GLOB,
    RAW_DATA_DIR,
    RAW_GLOBAL_CSV,
    SQL_DIR,
    ensure_project_dirs,
    )
except ImportError:
    from config import (
        BUILD_METADATA_PATH,
        CSV_EXPORT_TABLES,
        DB_PATH,
        EXPECTED_LOW_COVERAGE_COUNTRIES,
        EXPORT_DIR,
        LOW_COVERAGE_ROW_THRESHOLD,
        PARQUET_EXPORT_TABLES,
        RAW_COUNTRY_GLOB,
        RAW_DATA_DIR,
        RAW_GLOBAL_CSV,
        SQL_DIR,
        ensure_project_dirs,
    )


RAW_TABLE_SQL = """
CREATE OR REPLACE TABLE raw_nike_size AS
SELECT *
FROM read_csv(
    '{csv_path}',
    auto_detect = false,
    header = true,
    sample_size = -1,
    quote = '"',
    escape = '"',
    strict_mode = false,
    columns = {{
        'snapshot_date': 'DATE',
        'country_code': 'VARCHAR',
        'product_name': 'VARCHAR',
        'model_number': 'VARCHAR',
        'currency': 'VARCHAR',
        'price_local': 'DOUBLE',
        'sale_price_local': 'DOUBLE',
        'gender_segment': 'VARCHAR',
        'size_label': 'VARCHAR',
        'category': 'VARCHAR',
        'subcategory': 'VARCHAR',
        'product_id': 'VARCHAR',
        'sku': 'VARCHAR',
        'style_color': 'VARCHAR',
        'brand_name': 'VARCHAR',
        'color_name': 'VARCHAR',
        'size_count': 'VARCHAR',
        'available_size_count': 'VARCHAR',
        'available': 'BOOLEAN',
        'availability_level': 'VARCHAR',
        'available_market': 'BOOLEAN',
        'in_stock': 'BOOLEAN',
        'discount_pct': 'DOUBLE',
        'employee_price': 'DOUBLE',
        'product_url': 'VARCHAR',
        'canonical_url': 'VARCHAR',
        'image_url': 'VARCHAR',
        'gtin': 'VARCHAR',
        'stock_keeping_unit_id': 'VARCHAR',
        'catalog_sku_id': 'VARCHAR',
        'nike_size': 'VARCHAR',
        'localized_size': 'VARCHAR',
        'size_conversion_id': 'VARCHAR',
        'sport_tags': 'VARCHAR',
        'record_source': 'VARCHAR'
    }}
);
"""


def sql_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def load_sql(name: str, **params: object) -> str:
    text = (SQL_DIR / name).read_text(encoding="utf-8")
    return text.format(**params) if params else text


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))
    con.execute("PRAGMA threads=4;")
    return con


def resolve_raw_source() -> tuple[Path | str, str]:
    if RAW_GLOBAL_CSV.exists():
        return RAW_GLOBAL_CSV, "global_csv"

    country_files = sorted(RAW_DATA_DIR.glob("Nike_*.csv"))
    if country_files:
        return RAW_COUNTRY_GLOB, "country_csv_glob"

    raise FileNotFoundError(
        "Raw dataset not found. Checked:\n"
        f"- {RAW_GLOBAL_CSV}\n"
        f"- {RAW_COUNTRY_GLOB}"
    )


def execute_script(con: duckdb.DuckDBPyConnection, sql_text: str) -> None:
    con.execute(sql_text)


def export_tables(con: duckdb.DuckDBPyConnection) -> None:
    for table_name in PARQUET_EXPORT_TABLES:
        parquet_path = EXPORT_DIR / f"{table_name}.parquet"
        con.execute(
            f"COPY (SELECT * FROM {table_name}) TO '{sql_path(parquet_path)}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD);"
        )

    for table_name in CSV_EXPORT_TABLES:
        csv_path = EXPORT_DIR / f"{table_name}.csv"
        con.execute(
            f"COPY (SELECT * FROM {table_name}) TO '{sql_path(csv_path)}' "
            "(HEADER, DELIMITER ',');"
        )


def build_metadata(con: duckdb.DuckDBPyConnection, raw_source: Path | str, raw_source_kind: str) -> dict[str, object]:
    schema_checks = con.execute(
        "SELECT check_name, expected_value, actual_value, passed FROM schema_checks ORDER BY check_name"
    ).fetchall()
    coverage = con.execute(
        """
        SELECT country_code
        FROM country_coverage_summary
        WHERE is_low_coverage_country
        ORDER BY country_code
        """
    ).fetchall()
    snapshot_dates = con.execute(
        "SELECT DISTINCT CAST(snapshot_date AS VARCHAR) FROM raw_nike_size ORDER BY 1"
    ).fetchall()

    metadata = {
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "raw_source": str(raw_source),
        "raw_source_kind": raw_source_kind,
        "duckdb_path": str(DB_PATH),
        "snapshot_dates": [row[0] for row in snapshot_dates],
        "low_coverage_row_threshold": LOW_COVERAGE_ROW_THRESHOLD,
        "expected_low_coverage_countries": list(EXPECTED_LOW_COVERAGE_COUNTRIES),
        "detected_low_coverage_countries": [row[0] for row in coverage],
        "schema_checks": [
            {
                "check_name": row[0],
                "expected_value": row[1],
                "actual_value": row[2],
                "passed": bool(row[3]),
            }
            for row in schema_checks
        ],
        "exports": {
            "parquet": [f"{name}.parquet" for name in PARQUET_EXPORT_TABLES],
            "csv": [f"{name}.csv" for name in CSV_EXPORT_TABLES],
        },
    }
    BUILD_METADATA_PATH.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metadata


def main() -> None:
    ensure_project_dirs()
    raw_source, raw_source_kind = resolve_raw_source()

    con = connect()
    try:
        execute_script(con, RAW_TABLE_SQL.format(csv_path=sql_path(Path(raw_source) if isinstance(raw_source, Path) else Path(str(raw_source)))))
        execute_script(
            con,
            load_sql(
                "01_validate_schema.sql",
                low_coverage_threshold=LOW_COVERAGE_ROW_THRESHOLD,
            ),
        )
        execute_script(con, load_sql("02_build_clean_base.sql"))
        execute_script(con, load_sql("03_build_summary_tables.sql"))
        export_tables(con)
        metadata = build_metadata(con, raw_source=raw_source, raw_source_kind=raw_source_kind)
    finally:
        con.close()

    print("Build completed.")
    print(f"Raw source kind: {raw_source_kind}")
    print(f"Raw source: {raw_source}")
    print(f"DuckDB database: {DB_PATH}")
    print(f"Exports directory: {EXPORT_DIR}")
    print(
        "Detected low coverage countries:",
        ", ".join(metadata["detected_low_coverage_countries"]) or "None",
    )


if __name__ == "__main__":
    main()
