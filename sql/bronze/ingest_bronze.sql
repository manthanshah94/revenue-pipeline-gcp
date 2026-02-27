-- BRONZE LAYER: Raw ingestion from public dataset
-- Copies thelook_ecommerce tables into your own bronze dataset
-- No transformations -- raw data as-is

CREATE OR REPLACE TABLE `{project_id}.bronze.orders` AS
SELECT * FROM `bigquery-public-data.thelook_ecommerce.orders`;

CREATE OR REPLACE TABLE `{project_id}.bronze.order_items` AS
SELECT * FROM `bigquery-public-data.thelook_ecommerce.order_items`;

CREATE OR REPLACE TABLE `{project_id}.bronze.users` AS
SELECT * FROM `bigquery-public-data.thelook_ecommerce.users`;

CREATE OR REPLACE TABLE `{project_id}.bronze.products` AS
SELECT * FROM `bigquery-public-data.thelook_ecommerce.products`;