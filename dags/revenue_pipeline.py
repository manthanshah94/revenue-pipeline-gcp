"""
revenue_pipeline.py
End-to-end revenue leakage detection pipeline
Bronze -> Silver -> Gold -> Quality Checks -> Report
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import sys
import os

# Add scripts to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from data_quality_checks import run_checks

# Config
PROJECT_ID = "revenue-pipeline-gcp"  # change to your GCP project ID
SQL_DIR = os.path.join(os.path.dirname(__file__), '..', 'sql')

def load_sql(filepath: str) -> str:
    with open(filepath) as f:
        return f.read().replace('{project_id}', PROJECT_ID)

def run_bronze(**kwargs):
    client = bigquery.Client()
    tables = ['orders', 'order_items', 'users', 'products']
    for table in tables:
        sql = f"""
            CREATE OR REPLACE TABLE `{PROJECT_ID}.bronze.{table}` AS
            SELECT * FROM `bigquery-public-data.thelook_ecommerce.{table}`
        """
        print(f"Loading bronze.{table}...")
        client.query(sql).result()
        print(f"bronze.{table} done!")
    print("Bronze layer complete")

def run_silver(**kwargs):
    client = bigquery.Client()
    for filename in ['silver_customers.sql', 'silver_orders.sql',
                     'silver_order_items.sql', 'silver_products.sql']:
        sql = load_sql(f"{SQL_DIR}/silver/{filename}")
        print(f"Running {filename}...")
        client.query(sql).result()
    print("Silver layer complete")

def run_gold(**kwargs):
    client = bigquery.Client()
    for filename in ['gold_revenue_summary.sql',
                     'gold_customer_360.sql',
                     'gold_revenue_leakage.sql']:
        sql = load_sql(f"{SQL_DIR}/gold/{filename}")
        print(f"Running {filename}...")
        client.query(sql).result()
    print("Gold layer complete")

def run_quality_checks(**kwargs):
    run_checks(PROJECT_ID)

def generate_report(**kwargs):
    client = bigquery.Client()
    print("\n REVENUE LEAKAGE REPORT")
    print("=" * 50)

    # leakage summary
    query = f"""
        SELECT
            leakage_type,
            COUNT(*) as total_incidents,
            ROUND(SUM(leakage_amount), 2) as total_leakage
        FROM `{PROJECT_ID}.gold.revenue_leakage`
        GROUP BY leakage_type
        ORDER BY total_leakage DESC
    """
    results = client.query(query).result()
    for row in results:
        print(f"{row.leakage_type}: {row.total_incidents} incidents, ${row.total_leakage:,.2f} leakage")

    # top churned high value customers
    query2 = f"""
        SELECT full_name, total_spent, days_since_last_order
        FROM `{PROJECT_ID}.gold.customer_360`
        WHERE is_churned = true AND ltv_segment = 'high_value'
        ORDER BY total_spent DESC
        LIMIT 5
    """
    print("\n Top 5 Churned High Value Customers:")
    results2 = client.query(query2).result()
    for row in results2:
        print(f"  {row.full_name}: ${row.total_spent:,.2f} spent, inactive {row.days_since_last_order} days")

# DAG definition
default_args = {
    'owner': 'manthan',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='revenue_leakage_pipeline',
    default_args=default_args,
    description='End-to-end revenue leakage detection on GCP BigQuery',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'revenue', 'medallion', 'gold']
) as dag:

    t1 = PythonOperator(
        task_id='ingest_bronze',
        python_callable=run_bronze
    )

    t2 = PythonOperator(
        task_id='transform_silver',
        python_callable=run_silver
    )

    t3 = PythonOperator(
        task_id='transform_gold',
        python_callable=run_gold
    )

    t4 = PythonOperator(
        task_id='data_quality_checks',
        python_callable=run_quality_checks
    )

    t5 = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report
    )

    # Pipeline dependency chain
    t1 >> t2 >> t3 >> t4 >> t5