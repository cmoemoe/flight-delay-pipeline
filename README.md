# U.S. Flight Delay Analysis Pipeline

An end-to-end batch data pipeline that ingests 7 years of U.S. domestic flight data from the Bureau of Transportation Statistics, transforms it through a multi-layer dbt project, and serves an interactive Power BI dashboard with executive-level KPIs, airline scorecards, airport delay maps, and pipeline monitoring.

**84 months of data · 55M+ flights · 2019–2025**

---

## Architecture

```
BTS Public API (HTTPS)
    │
    ▼
Python ETL (download → validate → upload)
    │
    ▼
Google Cloud Storage  ←── raw data lake (Parquet-partitioned by year/month)
    │
    ▼
BigQuery (flights_raw.flights)  ←── append-mode monthly loads
    │
    ▼
dbt (staging → intermediate → marts)
    │
    ├── mart_airline_performance   ─┐
    ├── mart_airport_delays         ├── Power BI Dashboard
    └── mart_delay_causes          ─┘
```

Orchestrated by **Apache Airflow** on a monthly schedule. Every layer is containerised with Docker Compose for local reproducibility.

---


## dbt Lineage

```
raw.flights  (BigQuery)
    └── stg_flights  [view]
          └── int_flights_enriched  [ephemeral]
                ├── mart_airline_performance  [table]  → Power BI
                ├── mart_airport_delays       [table]  → Power BI
                └── mart_delay_causes         [table]  → Power BI
```

**Staging** — casts all BTS column types, renames to snake_case, filters diverted flights, derives `is_delayed` (BTS threshold: >15 min), derives `delay_category`, and deduplicates on flight identity key.

**Intermediate** — ephemeral CTE that joins cleaned flights to carrier and airport seed tables, enriching every record with full carrier name, carrier group (Legacy / Low-cost / Ultra low-cost / Regional), and airport lat/lon coordinates.

**Marts** — three production tables consumed directly by Power BI:
- `mart_airline_performance` — monthly on-time rate, cancellation rate, avg delay, delay cause totals, and MoM change per carrier
- `mart_airport_delays` — monthly delay rate, avg delay, and dominant delay cause per origin airport
- `mart_delay_causes` — monthly national breakdown of delay minutes by cause with percentage splits

---

## Airflow DAG

The `flight_delay_pipeline` DAG runs on the 1st of each month at 06:00 UTC, loading the prior month's data (accounting for BTS's ~30-day publishing lag).

```
get_period → download → validate → upload_gcs → load_bigquery → dbt_run → dbt_test
```

| Task | Description |
|---|---|
| `get_period` | Derives year/month from execution date or manual config override |
| `download` | Downloads BTS zip via HTTPS, extracts CSV (~300MB per month) |
| `validate` | Checks file exists, schema columns present, row count > 1,000 |
| `upload_gcs` | Uploads to `gs://bucket/raw/year=YYYY/month=MM/` (Hive-partitioned) |
| `load_bigquery` | Appends CSV to `flights_raw.flights` via BigQuery load job |
| `dbt_run` | Runs all dbt models — staging view + three mart tables |
| `dbt_test` | Runs dbt schema tests — not_null, unique, accepted_values |

Each task retries 2× with a 5-minute delay. Email alerts on failure.

---

## Dashboard

Four-page Power BI report connected to BigQuery via live import:

**Executive Summary** — Total flights, on-time rate, avg delay, cancellation rate KPIs. Monthly on-time trend by airline (2019–2025). On-time rate bar chart ranked by carrier. Delay causes donut chart.
![Executive Summary](docs/flight-delay-dashboard.png)

**Airline Performance** — Carrier scorecard table with MoM change. On-time rate bar chart. Stacked delay-minutes chart by cause and airline. Carrier slicer for cross-filtering.


**Airport Delays** — US map with bubbles sized by delay rate. Top 10 worst-delay airports bar chart. Full airport summary table with dominant delay cause. State slicer.

**Data Quality Log** — Months loaded, total rows, average on-time rate, last updated KPIs. Rows loaded per month trend. Pipeline run history table.


---

## Data Source

**Bureau of Transportation Statistics — On-Time Performance**
https://www.transtats.bts.gov/DL_SelectFields.aspx

Monthly CSV files covering all domestic U.S. flights. Available from 1987 to present with a ~30-day lag. Each month is ~300MB uncompressed, ~500K rows.

**Cancellation codes:**
- `A` — Carrier
- `B` — Weather
- `C` — National Air System
- `D` — Security

**Delay cause definitions:**
- **Carrier** — issues within the airline's control (crew, maintenance, fueling)
- **Late Aircraft** — prior flight using the same aircraft arrived late
- **NAS / ATC** — air traffic control, runway congestion, non-extreme weather
- **Weather** — extreme weather directly causing a delay
- **Security** — TSA screening issues, terminal evacuations

---

## Key Findings (2019–2025)

- **COVID collapse (2020)** — flight volume dropped ~65% in April–May 2020. Paradoxically, on-time rates *improved* during this period due to reduced congestion.
- **Recovery (2021–2022)** — volume rebounded but on-time performance lagged, with 2022 posting the worst on-time rates of the dataset.
- **Southwest meltdown (December 2022)** — Southwest's on-time rate collapsed to historic lows, visible as a clear outlier in the monthly trend.
- **Late aircraft dominates (~40%)** — the largest delay cause nationally, showing that delays self-propagate through the network across the day.
- **Best performers** — Hawaiian Airlines and Delta consistently lead on on-time rate across the 7-year period.
- **Worst airports for delays** — Newark (EWR) and San Francisco (SFO) consistently rank among the highest average delay times.

---

## Setup

### Prerequisites
- Windows with WSL2 (Ubuntu) + Docker Desktop
- GCP account (free tier sufficient)
- Power BI Desktop

### GCP Setup

1. Create a GCP project and note your **Project ID**
2. Enable the Cloud Storage and BigQuery APIs
3. Create a service account with roles: `BigQuery Data Editor`, `BigQuery Job User`, `Storage Object Admin`
4. Download the key as `credentials/gcp-key.json` (gitignored)
5. Create a GCS bucket and two BigQuery datasets: `flights_raw` and `flights_dbt`

### Local Setup (WSL2)

```bash
# Clone the repo
git clone https://github.com/cmoemoe/flight-delay-pipeline.git
cd flight-delay-pipeline

# Copy and fill in environment variables
cp .env.example .env
# Edit .env with your GCP project ID, bucket name, etc.

# Place your GCP service account key
mkdir -p credentials
# Copy gcp-key.json into credentials/

# Start Airflow
docker compose up airflow-init   # first time only
docker compose up -d

# Open Airflow UI at http://localhost:8080 (admin/admin)
```

### Load seeds (one-time)

```bash
docker compose cp dbt/seeds/airports.csv airflow-scheduler:/opt/airflow/dbt/seeds/airports.csv
docker compose cp dbt/seeds/carriers.csv airflow-scheduler:/opt/airflow/dbt/seeds/carriers.csv
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt seed \
   --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt --no-use-colors"
```

### Trigger a pipeline run

```bash
# Trigger a specific month
docker compose exec airflow-scheduler airflow dags trigger flight_delay_pipeline \
  --conf '{"year": 2023, "month": 6}' \
  --run-id manual_2023_06

# Backfill a full year
for month in 1 2 3 4 5 6 7 8 9 10 11 12; do
  docker compose exec airflow-scheduler airflow dags trigger flight_delay_pipeline \
    --conf "{\"year\": 2023, \"month\": $month}" \
    --run-id "backfill_2023_$(printf '%02d' $month)"
  sleep 10
done
```

### Environment variables

| Variable | Description |
|---|---|
| `GCP_PROJECT_ID` | GCP project ID |
| `GCS_BUCKET_NAME` | GCS bucket for raw data lake |
| `BIGQUERY_DATASET` | Raw dataset name (default: `flights_raw`) |
| `DBT_DATASET` | dbt output dataset (default: `flights_dbt`) |
| `ALERT_EMAIL` | Email for Airflow failure alerts |
