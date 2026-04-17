CREATE OR REPLACE TABLE cleaned_nike_size AS
WITH base AS (
    SELECT
        snapshot_date,
        UPPER(country_code) AS country_code,
        UPPER(currency) AS currency,
        UPPER(country_code) || '|' || UPPER(currency) AS market_key,
        regexp_replace(product_name, '^"+|"+$', '') AS product_name,
        product_name AS product_name_raw,
        NULLIF(model_number, '') AS model_number,
        NULLIF(product_id, '') AS product_id,
        NULLIF(sku, '') AS sku,
        NULLIF(style_color, '') AS style_color,
        NULLIF(brand_name, '') AS brand_name,
        NULLIF(color_name, '') AS color_name,
        UPPER(NULLIF(category, '')) AS category,
        NULLIF(subcategory, '') AS subcategory,
        UPPER(COALESCE(NULLIF(gender_segment, ''), 'UNKNOWN')) AS gender_segment_raw,
        CASE
            WHEN UPPER(COALESCE(gender_segment, '')) IN ('MEN', 'WOMEN', 'BOYS', 'GIRLS')
                THEN UPPER(gender_segment)
            WHEN UPPER(COALESCE(gender_segment, '')) IN ('MEN|WOMEN', 'WOMEN|MEN')
                THEN 'UNISEX_ADULT'
            WHEN UPPER(COALESCE(gender_segment, '')) IN ('BOYS|GIRLS', 'GIRLS|BOYS')
                THEN 'UNISEX_KIDS'
            WHEN UPPER(COALESCE(gender_segment, '')) IN (
                'MEN|BOYS|WOMEN|GIRLS',
                'BOYS|WOMEN|GIRLS',
                'MEN|BOYS|GIRLS',
                'MEN|WOMEN|GIRLS'
            )
                THEN 'MIXED_ALL'
            WHEN UPPER(COALESCE(gender_segment, '')) = 'WOMEN|GIRLS'
                THEN 'WOMEN_GIRLS'
            WHEN COALESCE(gender_segment, '') = ''
                THEN 'UNKNOWN'
            ELSE 'OTHER'
        END AS gender_segment_normalized,
        TRY_CAST(price_local AS DOUBLE) AS current_price_local,
        TRY_CAST(sale_price_local AS DOUBLE) AS reference_price_local_raw,
        TRY_CAST(discount_pct AS DOUBLE) AS reported_discount_pct,
        NULLIF(size_label, '') AS size_label,
        NULLIF(nike_size, '') AS nike_size,
        NULLIF(localized_size, '') AS localized_size,
        NULLIF(size_conversion_id, '') AS size_conversion_id,
        NULLIF(sport_tags, '') AS sport_tags,
        COALESCE(NULLIF(split_part(sport_tags, '|', 1), ''), 'Unassigned') AS primary_sport_tag,
        CASE
            WHEN CAST(available AS VARCHAR) IN ('true', 'TRUE', 'True', '1') THEN TRUE
            ELSE FALSE
        END AS available_size_flag,
        CASE
            WHEN CAST(available_market AS VARCHAR) IN ('true', 'TRUE', 'True', '1') THEN TRUE
            ELSE FALSE
        END AS available_market_flag,
        CASE
            WHEN CAST(in_stock AS VARCHAR) IN ('true', 'TRUE', 'True', '1') THEN TRUE
            ELSE FALSE
        END AS in_stock_flag,
        UPPER(COALESCE(NULLIF(availability_level, ''), 'UNKNOWN')) AS availability_level,
        NULLIF(product_url, '') AS product_url,
        NULLIF(canonical_url, '') AS canonical_url,
        NULLIF(image_url, '') AS image_url,
        NULLIF(record_source, '') AS record_source
    FROM raw_nike_size
),
normalized AS (
    SELECT
        *,
        CASE
            WHEN reference_price_local_raw IS NOT NULL
                AND current_price_local IS NOT NULL
                AND reference_price_local_raw > current_price_local
                AND reference_price_local_raw > 0
                THEN ROUND(((reference_price_local_raw - current_price_local) / reference_price_local_raw) * 100, 4)
            WHEN reported_discount_pct IS NOT NULL
                THEN ROUND(reported_discount_pct, 4)
            ELSE 0.0
        END AS normalized_discount_pct,
        CASE
            WHEN reference_price_local_raw IS NOT NULL
                AND current_price_local IS NOT NULL
                AND reference_price_local_raw > current_price_local
                THEN TRUE
            WHEN COALESCE(reported_discount_pct, 0) > 0
                THEN TRUE
            ELSE FALSE
        END AS has_discount,
        CASE
            WHEN reference_price_local_raw IS NOT NULL
                AND current_price_local IS NOT NULL
                AND reference_price_local_raw < current_price_local
                THEN TRUE
            WHEN COALESCE(reported_discount_pct, 0) < 0
                THEN TRUE
            ELSE FALSE
        END AS pricing_anomaly_flag
    FROM base
)
SELECT
    snapshot_date,
    country_code,
    currency,
    market_key,
    product_name,
    product_name_raw,
    model_number,
    product_id,
    sku,
    style_color,
    brand_name,
    color_name,
    category,
    subcategory,
    gender_segment_raw,
    gender_segment_normalized,
    current_price_local,
    reference_price_local_raw AS reference_price_local,
    normalized_discount_pct,
    has_discount,
    pricing_anomaly_flag,
    size_label,
    nike_size,
    localized_size,
    size_conversion_id,
    sport_tags,
    primary_sport_tag,
    available_size_flag,
    available_market_flag,
    in_stock_flag,
    availability_level,
    product_url,
    canonical_url,
    image_url,
    record_source
FROM normalized;
