-- SILVER LAYER: Order items enriched with product cost
CREATE OR REPLACE TABLE `{project_id}.silver.order_items` AS
SELECT
    oi.id                                       AS order_item_id,
    oi.order_id,
    oi.user_id                                  AS customer_id,
    oi.product_id,
    oi.status,
    CAST(oi.created_at AS DATE)                 AS order_date,
    oi.sale_price,
    p.cost,
    p.category,
    p.brand,
    p.name                                      AS product_name,
    p.retail_price,
    -- margin calculation
    ROUND(oi.sale_price - p.cost, 2)            AS gross_margin,
    ROUND((oi.sale_price - p.cost)
        / NULLIF(oi.sale_price, 0) * 100, 2)   AS margin_pct,
    -- sold below cost flag (revenue leakage signal)
    CASE WHEN oi.sale_price < p.cost            THEN true
         ELSE false END                         AS is_sold_below_cost
FROM `{project_id}.bronze.order_items` oi
LEFT JOIN `{project_id}.bronze.products` p
    ON oi.product_id = p.id
WHERE oi.id IS NOT NULL