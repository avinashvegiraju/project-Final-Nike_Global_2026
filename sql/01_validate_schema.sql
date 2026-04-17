CREATE OR REPLACE TABLE schema_checks AS
WITH column_count AS (
    SELECT
        'column_count' AS check_name,
        '35' AS expected_value,
        CAST(COUNT(*) AS VARCHAR) AS actual_value,
        COUNT(*) = 35 AS passed
    FROM information_schema.columns
    WHERE table_name = 'raw_nike_size'
),
snapshot_count AS (
    SELECT
        'snapshot_date_count' AS check_name,
        '1' AS expected_value,
        CAST(COUNT(DISTINCT snapshot_date) AS VARCHAR) AS actual_value,
        COUNT(DISTINCT snapshot_date) = 1 AS passed
    FROM raw_nike_size
),
snapshot_value AS (
    SELECT
        'snapshot_date_value' AS check_name,
        '2026-03-19' AS expected_value,
        COALESCE(string_agg(DISTINCT CAST(snapshot_date AS VARCHAR), ', '), 'NULL') AS actual_value,
        COUNT(DISTINCT snapshot_date) = 1
            AND MIN(snapshot_date) = DATE '2026-03-19'
            AND MAX(snapshot_date) = DATE '2026-03-19' AS passed
    FROM raw_nike_size
),
row_count AS (
    SELECT
        'raw_row_count' AS check_name,
        '>= 1000000' AS expected_value,
        CAST(COUNT(*) AS VARCHAR) AS actual_value,
        COUNT(*) >= 1000000 AS passed
    FROM raw_nike_size
)
SELECT * FROM column_count
UNION ALL
SELECT * FROM snapshot_count
UNION ALL
SELECT * FROM snapshot_value
UNION ALL
SELECT * FROM row_count;

CREATE OR REPLACE TABLE country_coverage_summary AS
SELECT
    country_code,
    COUNT(*) AS size_row_count,
    COUNT(DISTINCT product_id) AS unique_products,
    COUNT(DISTINCT model_number) AS unique_models,
    COUNT(*) < {low_coverage_threshold} AS is_low_coverage_country,
    CASE
        WHEN COUNT(*) < {low_coverage_threshold}
            THEN 'Low coverage country: exclude from peer rankings by default'
        ELSE 'Peer benchmark country'
    END AS coverage_note
FROM raw_nike_size
GROUP BY 1
ORDER BY size_row_count DESC;

CREATE OR REPLACE TABLE market_currency_coverage_summary AS
SELECT
    country_code,
    currency,
    country_code || '|' || currency AS market_key,
    COUNT(*) AS size_row_count,
    COUNT(DISTINCT product_id) AS unique_products,
    COUNT(*) < {low_coverage_threshold} AS is_sparse_market_currency
FROM raw_nike_size
GROUP BY 1, 2, 3
ORDER BY size_row_count DESC;
