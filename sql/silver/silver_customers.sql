-- SILVER LAYER: Cleaned and validated customers
CREATE OR REPLACE TABLE `{project_id}.silver.customers` AS
SELECT
    id                                          AS customer_id,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name)          AS full_name,
    email,
    LOWER(gender)                               AS gender,
    age,
    city,
    state,
    country,
    CAST(created_at AS DATE)                    AS signup_date,
    traffic_source,
    -- data quality flags
    CASE WHEN age < 18 OR age > 100             THEN true
         ELSE false END                         AS is_invalid_age,
    CASE WHEN email IS NULL                     THEN true
         ELSE false END                         AS is_missing_email
FROM `{project_id}.bronze.users`
WHERE id IS NOT NULL