"""
scheduler/pipeline_scheduler.py â€” APScheduler-based hourly pipeline orchestrator.

Runs the full ETL pipeline (ingest â†’ transform â†’ quality check â†’ load star schema)
on a configurable interval. Handles errors gracefully so the scheduler never crashes.
"""

import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger(__name__)


def run_full_pipeline() -> None:
    """
    Execute the complete ETL pipeline for one run.

    Steps:
      1. Ingest live weather data from OpenWeatherMap
      2. Load raw_weather from SQLite into a DataFrame
      3. Transform and enrich the DataFrame
      4. Run data quality checks and log the report
      5. Insert transformed data into the star schema (fact_weather)
      6. Log the pipeline run result
    """
    import sqlite3
    import pandas as pd
    from ingestion.api_fetcher import run_ingestion
    from processing.transformer import transform_raw_weather
    from processing.data_quality import run_quality_checks, log_quality_report
    from storage.db_writer import insert_fact_weather, log_pipeline_run, _get_conn

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    rows_fetched = 0
    rows_saved = 0
    status = "SUCCESS"
    error_msg = ""

    logger.info("=== Pipeline run %s STARTED at %s ===", run_id, started_at)

    try:
        # â”€â”€ Step 1: Ingest â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        rows_fetched = run_ingestion()

        # â”€â”€ Step 2: Load raw_weather into DataFrame â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        os.makedirs(os.path.dirname(config.DB_PATH) or ".", exist_ok=True)
        conn_raw = sqlite3.connect(config.DB_PATH)
        try:
            raw_df = pd.read_sql("SELECT * FROM raw_weather", conn_raw)
        finally:
            conn_raw.close()

        if raw_df.empty:
            logger.warning("raw_weather table is empty â€” skipping transform")
            status = "SUCCESS"
        else:
            # â”€â”€ Step 3: Transform â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            transformed_df = transform_raw_weather(raw_df)

            # â”€â”€ Step 4: Quality checks â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            report = run_quality_checks(transformed_df)
            log_quality_report(report, run_id)

            # â”€â”€ Step 5: Load star schema â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            star_conn = sqlite3.connect(config.DB_PATH)
            star_conn.row_factory = sqlite3.Row
            try:
                rows_saved = insert_fact_weather(star_conn, transformed_df)
            finally:
                star_conn.close()

    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        logger.error("Pipeline run %s FAILED: %s", run_id, exc, exc_info=True)

    finally:
        finished_at = datetime.now(timezone.utc).isoformat()
        log_pipeline_run(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            rows_fetched=rows_fetched,
            rows_saved=rows_saved,
            status=status,
            error=error_msg,
        )
        logger.info(
            "=== Pipeline run %s %s â€” fetched=%d, saved=%d ===",
            run_id,
            status,
            rows_fetched,
            rows_saved,
        )


def start_scheduler() -> None:
    """
    Start the APScheduler loop.

    Runs run_full_pipeline() immediately on startup, then repeats every
    FETCH_INTERVAL_MINUTES. Blocks until KeyboardInterrupt.
    """
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_full_pipeline,
        trigger="interval",
        minutes=config.FETCH_INTERVAL_MINUTES,
        id="weather_pipeline",
        name="Weather ETL Pipeline",
    )

    logger.info(
        "Scheduler configured â€” interval=%d minutes", config.FETCH_INTERVAL_MINUTES
    )

    # Run immediately on startup
    logger.info("Running pipeline immediately on startup â€¦")
    run_full_pipeline()

    scheduler.start()
    logger.info("Scheduler started. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received â€” shutting down scheduler â€¦")
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped cleanly.")

