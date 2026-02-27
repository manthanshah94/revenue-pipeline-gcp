-- GOLD LAYER: Daily revenue summary
CREATE OR REPLACE TABLE `{project_id}.gold.revenue_summary` AS
SELECT
    order_date,
    COUNT(DISTINCT order_id)                    AS total_orders,
    COUNT(order_item_id)                        AS total_items,
    ROUND(SUM(sale_price), 2)                   AS total_revenue,
    ROUND(SUM(gross_margin), 2)                 AS total_margin,
    ROUND(AVG(margin_pct), 2)                   AS avg_margin_pct,
    COUNT(CASE WHEN is_sold_below_cost
          THEN 1 END)                           AS items_sold_below_cost,
    ROUND(SUM(CASE WHEN is_sold_below_cost
          THEN ABS(gross_margin) ELSE 0 END)
    , 2)                                        AS revenue_leakage_amount
FROM `{project_id}.silver.order_items`
GROUP BY order_date
ORDER BY order_date DESC