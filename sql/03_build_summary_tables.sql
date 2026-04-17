CREATE OR REPLACE TABLE product_market_summary AS
WITH country_flags AS (
    SELECT
        country_code,
        is_low_coverage_country
    FROM country_coverage_summary
),
market_flags AS (
    SELECT
        market_key,
        size_row_count AS market_currency_size_rows,
        is_sparse_market_currency
    FROM market_currency_coverage_summary
)
SELECT
    c.snapshot_date,
    c.country_code,
    c.currency,
    c.market_key,
    c.product_id,
    MIN(c.model_number) AS model_number,
    MIN(c.product_name) AS product_name,
    MIN(c.style_color) AS style_color,
    MIN(c.brand_name) AS brand_name,
    MIN(c.color_name) AS color_name,
    MIN(c.category) AS category,
    MIN(c.subcategory) AS subcategory,
    MIN(c.gender_segment_raw) AS gender_segment_raw,
    MIN(c.gender_segment_normalized) AS gender_segment_normalized,
    MIN(c.sport_tags) AS sport_tags,
    MIN(c.primary_sport_tag) AS primary_sport_tag,
    ROUND(AVG(c.current_price_local), 4) AS current_price_local,
    MIN(c.reference_price_local) FILTER (WHERE c.reference_price_local IS NOT NULL) AS reference_price_local,
    ROUND(MAX(c.normalized_discount_pct), 4) AS normalized_discount_pct,
    bool_or(c.has_discount) AS has_discount,
    bool_or(c.pricing_anomaly_flag) AS pricing_anomaly_flag,
    COUNT(*) AS total_listed_sizes,
    SUM(CASE WHEN c.available_size_flag THEN 1 ELSE 0 END) AS available_sizes,
    SUM(CASE WHEN NOT c.available_size_flag THEN 1 ELSE 0 END) AS unavailable_sizes,
    ROUND(SUM(CASE WHEN c.available_size_flag THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS size_availability_rate,
    bool_or(c.available_market_flag) AS available_market_flag,
    bool_or(c.in_stock_flag) AS in_stock_product_flag,
    cf.is_low_coverage_country,
    mf.market_currency_size_rows,
    mf.is_sparse_market_currency,
    MIN(c.product_url) AS product_url,
    MIN(c.canonical_url) AS canonical_url,
    MIN(c.image_url) AS image_url
FROM cleaned_nike_size c
LEFT JOIN country_flags cf USING (country_code)
LEFT JOIN market_flags mf USING (market_key)
GROUP BY
    c.snapshot_date,
    c.country_code,
    c.currency,
    c.market_key,
    c.product_id,
    cf.is_low_coverage_country,
    mf.market_currency_size_rows,
    mf.is_sparse_market_currency;

CREATE OR REPLACE TABLE market_category_mix AS
WITH category_counts AS (
    SELECT
        snapshot_date,
        country_code,
        currency,
        market_key,
        is_low_coverage_country,
        category,
        COUNT(*) AS product_count
    FROM product_market_summary
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
    *,
    ROUND(product_count * 1.0 / SUM(product_count) OVER (PARTITION BY market_key), 4) AS product_share
FROM category_counts;

CREATE OR REPLACE TABLE market_gender_mix AS
WITH gender_counts AS (
    SELECT
        snapshot_date,
        country_code,
        currency,
        market_key,
        is_low_coverage_country,
        gender_segment_normalized,
        COUNT(*) AS product_count
    FROM product_market_summary
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
    *,
    ROUND(product_count * 1.0 / SUM(product_count) OVER (PARTITION BY market_key), 4) AS product_share
FROM gender_counts;

CREATE OR REPLACE TABLE market_sport_mix AS
WITH exploded AS (
    SELECT
        p.snapshot_date,
        p.country_code,
        p.currency,
        p.market_key,
        p.is_low_coverage_country,
        TRIM(sport_tag) AS sport_tag
    FROM product_market_summary p,
    UNNEST(string_split(COALESCE(p.sport_tags, ''), '|')) AS t(sport_tag)
    WHERE TRIM(sport_tag) <> ''
),
sport_counts AS (
    SELECT
        snapshot_date,
        country_code,
        currency,
        market_key,
        is_low_coverage_country,
        sport_tag,
        COUNT(*) AS product_count
    FROM exploded
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
    *,
    ROUND(product_count * 1.0 / SUM(product_count) OVER (PARTITION BY market_key), 4) AS product_share
FROM sport_counts;

CREATE OR REPLACE TABLE market_summary AS
WITH base AS (
    SELECT
        snapshot_date,
        country_code,
        currency,
        market_key,
        MIN(is_low_coverage_country) AS is_low_coverage_country,
        MIN(market_currency_size_rows) AS market_currency_size_rows,
        MIN(is_sparse_market_currency) AS is_sparse_market_currency,
        COUNT(*) AS product_market_rows,
        SUM(total_listed_sizes) AS total_size_rows,
        COUNT(DISTINCT product_id) AS unique_products,
        COUNT(DISTINCT model_number) AS unique_models,
        ROUND(AVG(size_availability_rate), 4) AS avg_size_availability_rate,
        ROUND(SUM(available_sizes) * 1.0 / NULLIF(SUM(total_listed_sizes), 0), 4) AS size_row_availability_rate,
        ROUND(AVG(CASE WHEN in_stock_product_flag THEN 1 ELSE 0 END), 4) AS in_stock_product_rate,
        ROUND(AVG(CASE WHEN available_market_flag THEN 1 ELSE 0 END), 4) AS available_market_rate,
        ROUND(AVG(CASE WHEN has_discount THEN 1 ELSE 0 END), 4) AS discount_product_rate,
        ROUND(AVG(normalized_discount_pct) FILTER (WHERE has_discount), 4) AS avg_discount_pct_among_discounted,
        ROUND(AVG(current_price_local), 4) AS avg_current_price_local,
        ROUND(MEDIAN(current_price_local), 4) AS median_current_price_local,
        ROUND(AVG(CASE WHEN pricing_anomaly_flag THEN 1 ELSE 0 END), 4) AS pricing_anomaly_rate
    FROM product_market_summary
    GROUP BY 1, 2, 3, 4
),
category_json AS (
    SELECT
        market_key,
        '{' || string_agg('"' || category || '": ' || CAST(ROUND(product_share * 100, 2) AS VARCHAR), ', ') || '}' AS category_mix_json
    FROM market_category_mix
    GROUP BY 1
),
gender_json AS (
    SELECT
        market_key,
        '{' || string_agg('"' || gender_segment_normalized || '": ' || CAST(ROUND(product_share * 100, 2) AS VARCHAR), ', ') || '}' AS gender_mix_json
    FROM market_gender_mix
    GROUP BY 1
),
sport_json AS (
    SELECT
        market_key,
        '{' || string_agg('"' || sport_tag || '": ' || CAST(ROUND(product_share * 100, 2) AS VARCHAR), ', ') || '}' AS sport_mix_json
    FROM market_sport_mix
    GROUP BY 1
)
SELECT
    b.*,
    cj.category_mix_json,
    gj.gender_mix_json,
    sj.sport_mix_json
FROM base b
LEFT JOIN category_json cj USING (market_key)
LEFT JOIN gender_json gj USING (market_key)
LEFT JOIN sport_json sj USING (market_key)
ORDER BY unique_products DESC;

CREATE OR REPLACE TABLE matched_product_price_benchmark AS
WITH comparable AS (
    SELECT
        *,
        COUNT(DISTINCT country_code) OVER (PARTITION BY product_id, currency) AS comparable_markets
    FROM product_market_summary
    WHERE current_price_local IS NOT NULL
        AND current_price_local > 0
),
filtered AS (
    SELECT *
    FROM comparable
    WHERE comparable_markets >= 2
)
SELECT
    snapshot_date,
    product_id,
    model_number,
    product_name,
    category,
    gender_segment_normalized,
    currency,
    country_code,
    market_key,
    comparable_markets,
    current_price_local,
    reference_price_local,
    normalized_discount_pct,
    has_discount,
    MIN(current_price_local) OVER (PARTITION BY product_id, currency) AS min_price_local,
    MEDIAN(current_price_local) OVER (PARTITION BY product_id, currency) AS median_price_local,
    MAX(current_price_local) OVER (PARTITION BY product_id, currency) AS max_price_local,
    ROUND(
        current_price_local - MEDIAN(current_price_local) OVER (PARTITION BY product_id, currency),
        4
    ) AS price_gap_to_median_local,
    ROUND(
        CASE
            WHEN MEDIAN(current_price_local) OVER (PARTITION BY product_id, currency) = 0 THEN NULL
            ELSE (
                (current_price_local - MEDIAN(current_price_local) OVER (PARTITION BY product_id, currency))
                / MEDIAN(current_price_local) OVER (PARTITION BY product_id, currency)
            ) * 100
        END,
        4
    ) AS price_index_vs_median_pct,
    ROUND(
        current_price_local - MIN(current_price_local) OVER (PARTITION BY product_id, currency),
        4
    ) AS price_gap_to_min_local,
    ROUND(
        CASE
            WHEN MIN(current_price_local) OVER (PARTITION BY product_id, currency) = 0 THEN NULL
            ELSE (
                (current_price_local - MIN(current_price_local) OVER (PARTITION BY product_id, currency))
                / MIN(current_price_local) OVER (PARTITION BY product_id, currency)
            ) * 100
        END,
        4
    ) AS price_premium_vs_min_pct,
    is_low_coverage_country,
    is_sparse_market_currency,
    image_url,
    product_url
FROM filtered;

CREATE OR REPLACE TABLE matched_price_group_summary AS
SELECT
    snapshot_date,
    product_id,
    model_number,
    product_name,
    category,
    currency,
    MAX(comparable_markets) AS comparable_markets,
    ROUND(MIN(current_price_local), 4) AS min_price_local,
    ROUND(MEDIAN(current_price_local), 4) AS median_price_local,
    ROUND(MAX(current_price_local), 4) AS max_price_local,
    ROUND(MAX(current_price_local) - MIN(current_price_local), 4) AS absolute_price_spread_local,
    ROUND(
        CASE
            WHEN MIN(current_price_local) = 0 THEN NULL
            ELSE ((MAX(current_price_local) - MIN(current_price_local)) / MIN(current_price_local)) * 100
        END,
        4
    ) AS spread_vs_min_pct,
    string_agg(country_code, ', ' ORDER BY current_price_local, country_code) AS compared_countries
FROM matched_product_price_benchmark
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY spread_vs_min_pct DESC NULLS LAST, absolute_price_spread_local DESC;
