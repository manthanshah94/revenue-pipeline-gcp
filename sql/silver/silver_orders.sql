-- SILVER LAYER: Cleaned and validated orders
CREATE OR REPLACE TABLE `{project_id}.silver.orders` AS
SELECT
    order_id,
    user_id                                     AS customer_id,
    status,
    CAST(created_at AS DATE)                    AS order_date,
    CAST(returned_at AS DATE)                   AS returned_date,
    CAST(shipped_at AS DATE)                    AS shipped_date,
    num_of_item,
    -- data quality flags
    CASE WHEN status NOT IN (
        'Complete','Cancelled','Returned',
        'Processing','Shipped')                 THEN true
         ELSE false END                         AS is_invalid_status,
    CASE WHEN returned_at IS NOT NULL           THEN true
         ELSE false END                         AS is_returned
FROM `{project_id}.bronze.orders`
WHERE order_id IS NOT NULL