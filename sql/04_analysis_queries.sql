-- Top peer markets by assortment breadth
SELECT
    country_code,
    currency,
    unique_products,
    unique_models,
    in_stock_product_rate,
    discount_product_rate
FROM market_summary
WHERE NOT is_low_coverage_country
ORDER BY unique_products DESC
LIMIT 15;

-- Highest stock exposure among peer markets
SELECT
    country_code,
    currency,
    unique_products,
    in_stock_product_rate,
    avg_size_availability_rate,
    size_row_availability_rate
FROM market_summary
WHERE NOT is_low_coverage_country
ORDER BY in_stock_product_rate DESC, avg_size_availability_rate DESC
LIMIT 15;

-- Category stock heatmap input
SELECT
    country_code,
    category,
    ROUND(AVG(size_availability_rate), 4) AS avg_size_availability_rate,
    ROUND(AVG(CASE WHEN in_stock_product_flag THEN 1 ELSE 0 END), 4) AS in_stock_product_rate
FROM product_market_summary
WHERE NOT is_low_coverage_country
GROUP BY 1, 2
ORDER BY country_code, category;

-- Same-currency price spread summary
SELECT
    currency,
    COUNT(*) AS matched_product_groups,
    ROUND(AVG(spread_vs_min_pct), 4) AS avg_spread_vs_min_pct,
    ROUND(MAX(spread_vs_min_pct), 4) AS max_spread_vs_min_pct
FROM matched_price_group_summary
GROUP BY 1
ORDER BY matched_product_groups DESC, avg_spread_vs_min_pct DESC;

-- Top price-dispersion products
SELECT
    product_name,
    currency,
    comparable_markets,
    min_price_local,
    median_price_local,
    max_price_local,
    spread_vs_min_pct,
    compared_countries
FROM matched_price_group_summary
ORDER BY spread_vs_min_pct DESC NULLS LAST
LIMIT 25;

-- Discount intensity by market
SELECT
    country_code,
    currency,
    discount_product_rate,
    avg_discount_pct_among_discounted,
    pricing_anomaly_rate
FROM market_summary
ORDER BY discount_product_rate DESC, avg_discount_pct_among_discounted DESC;
