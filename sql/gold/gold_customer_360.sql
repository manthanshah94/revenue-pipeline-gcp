-- GOLD LAYER: Customer 360 with churn and LTV signals
CREATE OR REPLACE TABLE `{project_id}.gold.customer_360` AS
WITH customer_orders AS (
    SELECT
        oi.customer_id,
        COUNT(DISTINCT oi.order_id)             AS total_orders,
        ROUND(SUM(oi.sale_price), 2)            AS total_spent,
        ROUND(AVG(oi.sale_price), 2)            AS avg_order_value,
        MAX(oi.order_date)                      AS last_order_date,
        MIN(oi.order_date)                      AS first_order_date,
        DATE_DIFF(MAX(oi.order_date),
            MIN(oi.order_date), DAY)            AS customer_lifespan_days,
        COUNT(CASE WHEN oi.status = 'Returned'
              THEN 1 END)                       AS total_returns,
        ROUND(SUM(oi.gross_margin), 2)          AS total_margin_generated
    FROM `{project_id}.silver.order_items` oi
    GROUP BY oi.customer_id
)
SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.gender,
    c.age,
    c.city,
    c.state,
    c.country,
    c.signup_date,
    c.traffic_source,
    -- order metrics
    COALESCE(o.total_orders, 0)                 AS total_orders,
    COALESCE(o.total_spent, 0)                  AS total_spent,
    COALESCE(o.avg_order_value, 0)              AS avg_order_value,
    o.last_order_date,
    o.first_order_date,
    COALESCE(o.customer_lifespan_days, 0)       AS customer_lifespan_days,
    COALESCE(o.total_returns, 0)                AS total_returns,
    COALESCE(o.total_margin_generated, 0)       AS total_margin_generated,
    -- churn signals
    DATE_DIFF(CURRENT_DATE(), o.last_order_date, DAY) AS days_since_last_order,
    CASE WHEN DATE_DIFF(CURRENT_DATE(),
        o.last_order_date, DAY) > 90            THEN true
         ELSE false END                         AS is_churned,
    -- LTV segments
    CASE
        WHEN COALESCE(o.total_spent, 0) > 500   THEN 'high_value'
        WHEN COALESCE(o.total_spent, 0) > 200   THEN 'mid_value'
        ELSE                                         'low_value'
    END                                         AS ltv_segment
FROM `{project_id}.silver.customers` c
LEFT JOIN customer_orders o
    ON c.customer_id = o.customer_id