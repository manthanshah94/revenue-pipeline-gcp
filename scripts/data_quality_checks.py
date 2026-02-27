"""
data_quality_checks.py
Runs assertions against silver and gold layers
Fails loudly if data quality issues are found
"""
from google.cloud import bigquery

def run_checks(project_id: str):
    client = bigquery.Client()
    checks = [
        {
            "name": "silver_customers_no_nulls",
            "query": f"""
                SELECT COUNT(*) as cnt
                FROM `{project_id}.silver.customers`
                WHERE customer_id IS NULL
            """,
            "expected": 0
        },
        {
            "name": "silver_orders_valid_status",
            "query": f"""
                SELECT COUNT(*) as cnt
                FROM `{project_id}.silver.orders`
                WHERE is_invalid_status = true
            """,
            "expected": 0
        },
        {
            "name": "gold_revenue_summary_has_data",
            "query": f"""
                SELECT COUNT(*) as cnt
                FROM `{project_id}.gold.revenue_summary`
            """,
            "expected_min": 1
        },
        {
            "name": "gold_leakage_has_signals",
            "query": f"""
                SELECT COUNT(*) as cnt
                FROM `{project_id}.gold.revenue_leakage`
            """,
            "expected_min": 1
        },
    ]

    print("\n Running data quality checks...")
    all_passed = True

    for check in checks:
        result = client.query(check["query"]).result()
        count = list(result)[0].cnt

        if "expected" in check and count != check["expected"]:
            print(f"   FAIL: {check['name']} — expected {check['expected']}, got {count}")
            all_passed = False
        elif "expected_min" in check and count < check["expected_min"]:
            print(f"   FAIL: {check['name']} — expected at least {check['expected_min']}, got {count}")
            all_passed = False
        else:
            print(f"   PASS: {check['name']} ({count} rows)")

    if all_passed:
        print("\n All quality checks passed!")
    else:
        raise ValueError("Data quality checks failed. Pipeline halted.")