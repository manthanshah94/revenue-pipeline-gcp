-- GOLD LAYER: THE MONEY TABLE
-- Revenue leakage signals across 3 dimensions
CREATE OR REPLACE TABLE `{project_id}.gold.revenue_leakage` AS

-- SIGNAL 1: Items sold below cost
SELECT
    'sold_below_cost'                           AS leakage_type,
    CAST(order_item_id AS STRING)               AS entity_id,
    order_date,
    category,
    brand,
    product_name,
    sale_price,
    cost,
    ROUND(ABS(gross_margin), 2)                 AS leakage_amount,
    CONCAT('Item sold at ', CAST(sale_price AS STRING),
        ' below cost of ', CAST(cost AS STRING)) AS leakage_reason
FROM `{project_id}.silver.order_items`
WHERE is_sold_below_cost = true

UNION ALL

-- SIGNAL 2: High return rate customers
SELECT
    'high_return_customer'                      AS leakage_type,
    CAST(customer_id AS STRING)                 AS entity_id,
    last_order_date                             AS order_date,
    NULL                                        AS category,
    NULL                                        AS brand,
    full_name                                   AS product_name,
    total_spent                                 AS sale_price,
    NULL                                        AS cost,
    ROUND(total_returns * avg_order_value, 2)   AS leakage_amount,
    CONCAT('Customer has ', CAST(total_returns AS STRING),
        ' returns out of ',
        CAST(total_orders AS STRING),
        ' orders')                              AS leakage_reason
FROM `{project_id}.gold.customer_360`
WHERE total_orders > 0
    AND SAFE_DIVIDE(total_returns, total_orders) > 0.3

UNION ALL

-- SIGNAL 3: High value churned customers
SELECT
    'churned_high_value'                        AS leakage_type,
    CAST(customer_id AS STRING)                 AS entity_id,
    last_order_date                             AS order_date,
    NULL                                        AS category,
    NULL                                        AS brand,
    full_name                                   AS product_name,
    total_spent                                 AS sale_price,
    NULL                                        AS cost,
    total_spent                                 AS leakage_amount,
    CONCAT('High value customer inactive for ',
        CAST(days_since_last_order AS STRING),
        ' days')                                AS leakage_reason
FROM `{project_id}.gold.customer_360`
WHERE is_churned = true
    AND ltv_segment = 'high_value'