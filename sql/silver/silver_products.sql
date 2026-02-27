-- SILVER LAYER: Clean product catalog
CREATE OR REPLACE TABLE `{project_id}.silver.products` AS
SELECT
    id                                          AS product_id,
    name                                        AS product_name,
    category,
    brand,
    department,
    ROUND(cost, 2)                              AS cost,
    ROUND(retail_price, 2)                      AS retail_price,
    ROUND(retail_price - cost, 2)               AS expected_margin,
    ROUND((retail_price - cost)
        / NULLIF(retail_price, 0) * 100, 2)    AS expected_margin_pct,
    -- price tier
    CASE
        WHEN retail_price >= 200                THEN 'premium'
        WHEN retail_price >= 50                 THEN 'mid_range'
        ELSE                                         'budget'
    END                                         AS price_tier
FROM `{project_id}.bronze.products`
WHERE id IS NOT NULL
    AND cost IS NOT NULL
    AND retail_price IS NOT NULL