
# Travel Industry Data Migration Pipeline
### Hadoop → AWS S3 → Snowflake via EMR Serverless + Airflow

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.8-red)
![Spark](https://img.shields.io/badge/PySpark-EMR_Serverless-orange)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-cyan)
![Terraform](https://img.shields.io/badge/Terraform-IaC-purple)
![AWS](https://img.shields.io/badge/AWS-S3_|_EMR-yellow)

---

## Overview

This project simulates a **real-world data migration** at a travel industry company — moving hotel booking data from a legacy Hadoop-based system into a modern cloud data warehouse on Snowflake. The pipeline is designed to reflect the kind of data engineering work found in large-scale OTA (Online Travel Agency) platforms.

The key engineering challenges addressed:

- **Dirty source data** — null values, malformed dates, invalid status codes injected intentionally to simulate real HDFS exports
- **Multi-layer data quality gates** — validation at ingestion, at transformation, and at load time
- **Parallel Spark jobs** — three independent EMR Serverless jobs running concurrently for hotels, customers, and bookings
- **Full observability** — structured logs, SLO checks, and an auto-generated HTML monitoring dashboard
- **Infrastructure as Code** — all AWS resources provisioned via Terraform

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Apache Airflow (Docker)                       │
│                                                                       │
│  generate_data → ingest_to_s3 ──┬── transform_hotels   (EMR Spark)  │
│                                  ├── transform_customers (EMR Spark)  │
│                                  └── transform_bookings  (EMR Spark)  │
│                                          │                            │
│                               load_snowflake_raw                      │
│                               load_snowflake_staging                  │
│                               build_analytics_table                   │
│                               run_final_report                        │
└─────────────────────────────────────────────────────────────────────┘

Local HDFS Simulation          AWS Data Lake (S3)         Snowflake
┌──────────────┐     boto3    ┌─────────────────┐    COPY INTO
│ hotels.parq  │ ──────────→  │ raw/hotels/      │ ──────────→  RAW_DB
│ customers.pq │              │ raw/customers/   │              STAGING_DB
│ bookings.pq  │              │ raw/bookings/    │              ERROR_DB
└──────────────┘              │                 │              ANALYTICS_DB
                              │ staging/         │
         EMR Serverless ────→ │ staging/errors/  │
         (PySpark jobs)       └─────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8 (Docker Compose) |
| Transformation | PySpark on AWS EMR Serverless |
| Data Lake | AWS S3 (raw / staging / errors layers) |
| Data Warehouse | Snowflake (RAW → STAGING → ANALYTICS) |
| Infrastructure | Terraform |
| Language | Python 3.12 |
| Data Generation | Faker + Pandas |
| Monitoring | Plotly (self-contained HTML dashboard) |

---

## Data Model

### Source (RAW_DB) — mirrors legacy Hadoop schema

| Table | Rows | Intentional Dirty Data |
|---|---|---|
| hotels | 50 | 3 null cities, 2 zero star ratings |
| customers | 200 | 10 null emails, 5 malformed dates |
| bookings | 500 | 20 invalid status codes, 10 null prices |

### Cleaned (STAGING_DB) — post-Spark transformation

- `signup_date`, `check_in`, `check_out`: STRING → DATE
- `booking_status` INT → `status_label` VARCHAR (CONFIRMED / CANCELLED / PENDING)
- `stay_duration`: derived column (check_out − check_in in days)
- Rejected rows routed to **ERROR_DB** with `error_reason` and `error_timestamp`

### Analytics (ANALYTICS_DB)

`bookings_enriched` — a single denormalised wide table joining bookings + customers + hotels, ready for BI tools with no runtime JOINs required.

---

## Data Quality Gates

Three quality gates enforce data integrity at each pipeline stage:

**Quality Gate 1 — Pre-ingestion (ingest_to_s3.py)**
- File exists and is non-empty
- Row count meets minimum threshold (hotels ≥ 40, customers ≥ 150, bookings ≥ 400)
- Failure raises exception → Airflow marks task as FAILED, pipeline stops

**Quality Gate 2 — Pre-transformation (inside each Spark job)**
- All expected columns are present in the input DataFrame
- Null counts per column logged before transformation begins

**Quality Gate 3 — Post-load reconciliation (final_report.py)**
- Staging row count / source row count ≥ 90% (configurable)
- Error rate ≤ 10% per table
- SLO PASSED / FAILED status per table

---

## Monitoring

After each successful pipeline run, an HTML dashboard is auto-generated at `monitoring/report_{timestamp}.html`.

> **📸 Screenshot placeholder — add `monitoring/report_*.html` preview here**

The dashboard contains:
1. **Row count comparison** — source vs staging vs error rows per table
2. **Step duration chart** — time spent per pipeline task
3. **SLO status table** — green/red pass/fail per table

The Airflow DAG graph view shows the full task dependency tree and per-task status:

> **📸 Screenshot placeholder — add Airflow DAG graph view (all green) here**

---

## Project Structure

```
booking_demo/
├── dags/
│   └── migration_dag.py          # Airflow DAG definition
├── data_generator/
│   └── generate_data.py          # Synthetic data with intentional dirty records
├── ingestion/
│   └── ingest_to_s3.py           # Quality Gate 1 + S3 upload
├── spark_jobs/
│   ├── transform_hotels.py       # PySpark: clean + route errors
│   ├── transform_customers.py
│   └── transform_bookings.py
├── snowflake/ddl/
│   ├── 01_raw_tables.sql
│   ├── 02_staging_tables.sql
│   ├── 03_analytics_table.sql
│   └── 04_error_table.sql
├── validation/
│   ├── quality_gates.py          # Reusable QA functions
│   └── final_report.py           # SLO checks + dashboard trigger
├── monitoring/
│   └── generate_dashboard.py     # Plotly HTML dashboard
├── terraform/
│   ├── main.tf                   # S3, IAM, EMR Serverless resources
│   ├── variables.tf
│   └── outputs.tf
├── docker-compose.yml
├── Makefile                      # One-command setup
├── requirements.txt
├── .env.example
└── ERRORS_AND_FIXES.md
```

---

## Prerequisites

- Python 3.12
- Docker Desktop
- Terraform ≥ 1.0
- AWS account with programmatic access
- Snowflake account (free trial works)
- AWS CLI configured

---

## Quick Start

### 1. Clone and configure

```bash
git clone <repo-url>
cd <your project>
cp .env.example .env
```

### 2. Provision AWS infrastructure with Terraform

```bash
make infra-init
make infra-apply        # automatically syncs outputs → .env
```

### 3. Set up Snowflake

Run the DDL files in order in a Snowflake Worksheet:

```
snowflake/ddl/01_raw_tables.sql
snowflake/ddl/02_staging_tables.sql
snowflake/ddl/03_analytics_table.sql
snowflake/ddl/04_error_table.sql
```

> **Note:** Before running `01_raw_tables.sql`, complete the Snowflake Storage Integration setup. See [AWS_SETUP.md](AWS_SETUP.md) for the full IAM trust policy update steps.

Fill in Snowflake credentials in `.env`:
```
SNOWFLAKE_ACCOUNT=<account>.eu-west-3.aws
SNOWFLAKE_USER=<username>
SNOWFLAKE_PASSWORD=<password>
```

### 4. Generate data and upload Spark scripts

```bash
source .venv/bin/activate       # or python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
make generate-data
make upload-scripts
```

### 5. Start Airflow

```bash
make airflow-init               # first-time DB setup — wait for exit 0
make airflow-start
```

Open [http://localhost:8080](http://localhost:8080) (admin / admin), enable and trigger the `travel_migration_demo` DAG.

### 6. View results

- **Airflow UI**: watch each task turn green in the Graph view
- **Snowflake**: query `SELECT COUNT(*) FROM ANALYTICS_DB.PUBLIC.bookings_enriched`
- **Dashboard**: open `monitoring/report_*.html` in a browser

---

## Expected Pipeline Output

```
hotels    :  50 source →  45 staging |  5 errors  (SLO: PASSED)
customers : 200 source → 185 staging | 15 errors  (SLO: PASSED)
bookings  : 500 source → 470 staging | 30 errors  (SLO: PASSED)

Total bookings in ANALYTICS_DB: ~470  ✓
```

> **📸 Screenshot placeholder — add final_report task log output here**

---

## Makefile Reference

| Command | Description |
|---|---|
| `make setup` | Full one-command setup (infra + data + Airflow) |
| `make infra-apply` | Terraform apply + sync outputs to `.env` |
| `make env-sync` | Sync Terraform outputs to `.env` only |
| `make generate-data` | Generate synthetic Parquet files locally |
| `make upload-scripts` | Upload Spark scripts to S3 |
| `make airflow-restart` | Restart Airflow containers |
| `make infra-destroy` | Tear down all AWS resources |

---

## Common Errors & Fixes

See [ERRORS_AND_FIXES.md](ERRORS_AND_FIXES.md) for a full list of errors encountered and how to fix them, including:

- Python / pip version mismatch
- EMR Serverless region mismatch (S3 and EMR must be in the same region)
- Snowflake account identifier format
- Spark jobs not inheriting environment variables from Airflow
- Plotly subplot type incompatibility

---

## Screenshots to Add Before Publishing

| Location in README | What to capture |
|---|---|
| Monitoring section | `monitoring/report_*.html` open in browser — full dashboard |
| Monitoring section | Airflow DAG graph view with all tasks green |
| Expected output section | `run_final_report` task log in Airflow showing the reconciliation report |
| Snowflake (optional) | Snowflake worksheet showing row counts in ANALYTICS_DB |

---

## Author

Celine Li — Data Engineer  
[LinkedIn](#) · [GitHub](#)
