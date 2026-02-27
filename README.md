# Revenue Leakage Detection Pipeline — Airflow + GCP BigQuery

A production-style ELT pipeline built with **Apache Airflow** and **GCP BigQuery** 
that detects revenue leakage signals from real ecommerce transaction data.

> All data used is Google's public dataset `thelook_ecommerce` — 
> no proprietary data involved.

## Pipeline Architecture
```
ingest_bronze → transform_silver → transform_gold → data_quality_checks → generate_report
```

## Medallion Layers

| Layer | Dataset | Description |
|-------|---------|-------------|
| 🥉 Bronze | `bronze.*` | Raw ingestion from public dataset, no transformations |
| 🥈 Silver | `silver.*` | Cleaned, typed, joined with margin calculations |
| 🥇 Gold | `gold.*` | Business-ready leakage signals and customer 360 |

## Revenue Leakage Signals Detected

- Items sold below cost
- High return rate customers (30%+ return rate)
- Churned high value customers (inactive 90+ days)

## Results on Real Data

- High return customers: 10,885 incidents, $1,021,035.72 in leakage
- Churned high value customers: 1,431 incidents, $972,848.49 at risk

## Tech Stack

- Apache Airflow 3.x
- GCP BigQuery
- Python 3.10
- SQL

## Project Structure
```
├── dags/
│   └── revenue_pipeline.py    # Airflow DAG
├── sql/
│   ├── bronze/                # Raw ingestion SQL
│   ├── silver/                # Cleaning + transformation SQL
│   └── gold/                  # Business logic SQL
├── scripts/
│   └── data_quality_checks.py # Data quality assertions
└── requirements.txt
```

## Setup
```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/revenue-pipeline-gcp.git
cd revenue-pipeline-gcp

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set up GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"

# Set up Airflow
export AIRFLOW_HOME="$(pwd)/airflow"
airflow db migrate
airflow standalone

# Or run pipeline directly without Airflow
python3 -c "
import sys
sys.path.insert(0, 'scripts')
sys.path.insert(0, 'dags')
from revenue_pipeline import run_bronze, run_silver, run_gold, run_quality_checks, generate_report
run_bronze()
run_silver()
run_gold()
run_quality_checks()
generate_report()
"
```

## Part of my #BuildingInPublic series
Building in public — one Data Engineering or AI/ML concept every few days.