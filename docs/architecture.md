# Architecture — Real-Time Weather Analytics Pipeline

## Project Overview

The Real-Time Weather Analytics Pipeline is a production-style data engineering project
that ingests live and historical weather data for three major Indian cities (Mumbai, Delhi,
and Bangalore), transforms and enriches it, stores it in both a relational star schema
(SQLite) and a columnar data lake (Parquet), and visualises the results through a
multi-page Streamlit dashboard. The pipeline is fully automated via APScheduler, firing
every hour without any manual intervention.

---

## Pipeline Stages (5-Stage ETL)

### Stage 1 — Ingestion (`ingestion/`)

The ingestion stage is the entry point for all weather data.  `api_fetcher.py` calls the
OpenWeatherMap REST API for two endpoints per city:

* **`/weather`** — current conditions (temperature, humidity, wind, visibility, etc.)
* **`/forecast`** — 5-day / 3-hourly forecast slots

All HTTP calls carry a 10-second timeout and are wrapped in `try/except` blocks so a
single city failure never aborts the whole run.  `batch_loader.py` handles the offline
path: reading arbitrary CSV files, normalising column name variants (common across Kaggle
datasets), and bulk-inserting into the `historical_weather` table.

### Stage 2 — Processing (`processing/`)

`transformer.py` consumes a raw DataFrame and adds four derived columns:

| Column | Formula |
|---|---|
| `heat_index_c` | Steadman formula (valid: temp ≥ 27 °C, humidity ≥ 40 %) |
| `wind_chill_c` | Environment Canada formula (valid: temp ≤ 10 °C, wind ≥ 1.3 m/s) |
| `comfort_level` | Hot / Warm / Pleasant / Cold classification |
| `hour`, `day_of_week`, `is_daytime` | Temporal features extracted from timestamp |

`data_quality.py` evaluates the DataFrame for null completeness, duplicates, and
physically impossible temperature/humidity values.  It computes a 0–100 quality score
(threshold: ≥ 80 = pass) and persists the audit record to the `quality_log` table.

### Stage 3 — Storage (`storage/`)

`schema.sql` defines the full relational schema.  `db_writer.py` handles:

* **Dimension upserts** — idempotent `INSERT OR IGNORE` into `dim_city`, `dim_date`,
  and `dim_condition`.
* **Fact inserts** — resolves all FK references then appends to `fact_weather`.
* **Raw/log appends** — bulk pandas `to_sql` for `raw_weather` and `pipeline_log`.

A separate Parquet export is written to `data/parquet/` after every ingestion run for
columnar analysis with tools like DuckDB or Polars.

### Stage 4 — Scheduling (`scheduler/`)

`pipeline_scheduler.py` uses APScheduler's `BackgroundScheduler` to fire
`run_full_pipeline()` on a configurable interval (default: 60 minutes).  A run
immediately executes on startup before the scheduler loop begins.  All exceptions are
caught, logged, and recorded in `pipeline_log` without terminating the scheduler process.

### Stage 5 — Dashboard (`dashboard/`)

A four-page Streamlit application served from `dashboard/app.py`:

1. **Live Conditions** — card-per-city with latest reading, auto-refreshes every 60 s.
2. **Historical Trends** — temperature / humidity / wind charts filtered by city & date.
3. **City Comparison** — grouped bar, scatter plot, and summary stats table.
4. **Data Quality & Logs** — quality score timeline and last 20 pipeline run records.

---

## Star Schema Design

```
          dim_city ──────────┐
         (city_id PK)        │
                             ▼
dim_date ──────────► fact_weather ◄────── dim_condition
(date_id PK)        (FK: city_id,         (condition_id PK)
                     date_id,
                     condition_id)
```

| Table | Purpose |
|---|---|
| `dim_city` | One row per city with coordinates |
| `dim_date` | One row per calendar date, carries year/month/day/weekday flags |
| `dim_condition` | Deduplicated (weather_main, weather_desc, comfort_level) tuples |
| `fact_weather` | Hourly measurement grain; all FKs to dimensions |
| `raw_weather` | Schema-free raw API dump for re-processing |
| `quality_log` | One row per pipeline run with DQ metrics |
| `pipeline_log` | Execution audit: start/end timestamps, row counts, status |

---

## Data Quality Approach

Quality is assessed immediately after transformation, before the star schema load.
The `run_quality_checks()` function returns a report dict covering:

* **Completeness** — null counts per column, with critical columns weighted more heavily
* **Uniqueness** — duplicate row count
* **Validity** — temperature outside −20 / +60 °C range; humidity outside 0–100 % range
* **Quality score** — starts at 100, penalty deductions for each failure category (max
  penalties: nulls 30 pt, dups 20 pt, temp anomalies 30 pt, humidity anomalies 20 pt)
* **Pass/fail gate** — score ≥ 80 = PASS; result stored in `quality_log`

---

## APScheduler vs Airflow

Apache Airflow is the industry standard for DAG-based orchestration but requires a
Postgres/MySQL metastore, a web server, workers, and a scheduler process — a significant
operational overhead for a self-contained project.  APScheduler runs in-process with zero
external dependencies: a single `BackgroundScheduler` object manages job state in memory,
making it trivial to package and run on any laptop or small VM.  For a project with one
recurring job on a fixed interval, APScheduler provides 100 % of the functionality at a
fraction of the complexity.

---

## Free Tools Used

| Tool | Purpose | Rationale |
|---|---|---|
| **OpenWeatherMap API** | Real-time weather source | Free tier: 60 calls/min, no CC required |
| **SQLite** | Relational star schema | Zero-server, file-based, ships with Python |
| **Parquet + PyArrow** | Columnar data lake | Industry-standard format, excellent compression |
| **pandas** | DataFrame ETL engine | De-facto Python data wrangling standard |
| **APScheduler** | Pipeline orchestration | Lightweight, in-process, no infra required |
| **Streamlit** | Dashboard UI | Rapid Python-native dashboarding |
| **Plotly** | Interactive charts | Rich interactivity, works natively with Streamlit |
| **python-dotenv** | Secret management | Keeps API keys out of source code |
