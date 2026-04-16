"""
storage/db_writer.py — SQLite database initialisation and write helpers.

Uses sqlite3 for schema/dimension management and pandas for bulk appends.
All connections are explicitly closed after use.
"""

import os
import sqlite3
import logging
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

logger = logging.getLogger(__name__)

# ── Path to schema file ───────────────────────────────────────────────────────
_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_conn() -> sqlite3.Connection:
    """Return a new SQLite connection with row_factory set."""
    os.makedirs(os.path.dirname(config.DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# ── Public API ───────────────────────────────────────────────────────────────

def initialize_db() -> None:
    """Create all tables defined in schema.sql if they do not already exist."""
    os.makedirs(os.path.dirname(config.DB_PATH) or ".", exist_ok=True)
    with open(_SCHEMA_PATH, "r", encoding="utf-8") as fh:
        sql = fh.read()
    conn = _get_conn()
    try:
        conn.executescript(sql)
        conn.commit()
        logger.info("Database initialised at %s", config.DB_PATH)
    finally:
        conn.close()


def upsert_dim_city(conn: sqlite3.Connection, cities_list: List[Dict]) -> None:
    """Insert cities from config into dim_city if not already present."""
    for city in cities_list:
        conn.execute(
            """
            INSERT OR IGNORE INTO dim_city (city_name, country, latitude, longitude)
            VALUES (?, 'IN', ?, ?)
            """,
            (city["name"], city["lat"], city["lon"]),
        )
    conn.commit()
    logger.debug("dim_city upserted with %d cities", len(cities_list))


def upsert_dim_date(conn: sqlite3.Connection, date_str: str) -> int:
    """Insert date record into dim_date if absent; return its date_id."""
    try:
        dt = datetime.fromisoformat(date_str[:10])
    except ValueError:
        dt = datetime.utcnow()

    day_name = dt.strftime("%A")
    is_weekend = 1 if dt.weekday() >= 5 else 0

    conn.execute(
        """
        INSERT OR IGNORE INTO dim_date (full_date, year, month, day, day_of_week, is_weekend)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (dt.strftime("%Y-%m-%d"), dt.year, dt.month, dt.day, day_name, is_weekend),
    )
    conn.commit()

    row = conn.execute(
        "SELECT date_id FROM dim_date WHERE full_date = ?",
        (dt.strftime("%Y-%m-%d"),),
    ).fetchone()
    return row["date_id"]


def upsert_dim_condition(
    conn: sqlite3.Connection,
    weather_main: str,
    weather_desc: str,
    comfort_level: str,
) -> int:
    """Insert weather condition into dim_condition if absent; return condition_id."""
    conn.execute(
        """
        INSERT OR IGNORE INTO dim_condition (weather_main, weather_desc, comfort_level)
        VALUES (?, ?, ?)
        """,
        (weather_main, weather_desc, comfort_level),
    )
    conn.commit()

    row = conn.execute(
        """
        SELECT condition_id FROM dim_condition
        WHERE weather_main = ? AND weather_desc = ? AND comfort_level = ?
        """,
        (weather_main, weather_desc, comfort_level),
    ).fetchone()
    return row["condition_id"]


def insert_fact_weather(conn: sqlite3.Connection, transformed_df: pd.DataFrame) -> int:
    """Populate dimension tables and insert rows into fact_weather; return row count."""
    if transformed_df.empty:
        logger.warning("insert_fact_weather called with empty DataFrame")
        return 0

    upsert_dim_city(conn, config.CITIES)

    rows_inserted = 0
    for _, row in transformed_df.iterrows():
        try:
            # Resolve city_id
            city_row = conn.execute(
                "SELECT city_id FROM dim_city WHERE city_name = ?", (row.get("city"),)
            ).fetchone()
            city_id: Optional[int] = city_row["city_id"] if city_row else None

            # Resolve date_id
            date_id = upsert_dim_date(conn, str(row.get("timestamp", "")))

            # Resolve condition_id
            condition_id = upsert_dim_condition(
                conn,
                str(row.get("weather_main", "")),
                str(row.get("weather_desc", "")),
                str(row.get("comfort_level", "")),
            )

            conn.execute(
                """
                INSERT INTO fact_weather (
                    city_id, date_id, condition_id, timestamp,
                    temp_c, feels_like_c, temp_min_c, temp_max_c,
                    humidity_pct, pressure_hpa, wind_speed_ms, wind_deg,
                    visibility_m, cloud_pct, heat_index_c, wind_chill_c,
                    is_daytime, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    city_id,
                    date_id,
                    condition_id,
                    str(row.get("timestamp", "")),
                    row.get("temp_c"),
                    row.get("feels_like_c"),
                    row.get("temp_min_c"),
                    row.get("temp_max_c"),
                    row.get("humidity_pct"),
                    row.get("pressure_hpa"),
                    row.get("wind_speed_ms"),
                    row.get("wind_deg"),
                    row.get("visibility_m"),
                    row.get("cloud_pct"),
                    row.get("heat_index_c"),
                    row.get("wind_chill_c"),
                    int(bool(row.get("is_daytime", False))),
                    str(row.get("fetched_at", "")),
                ),
            )
            rows_inserted += 1
        except Exception as exc:
            logger.error("Failed inserting fact row for city=%s: %s", row.get("city"), exc)

    conn.commit()
    logger.info("Inserted %d rows into fact_weather", rows_inserted)
    return rows_inserted


def save_raw(records_list: List[Dict], table_name: str) -> None:
    """Append a list of dicts as rows to the given SQLite table."""
    if not records_list:
        logger.warning("save_raw called with empty list for table '%s'", table_name)
        return

    df = pd.DataFrame(records_list)
    conn = _get_conn()
    try:
        df.to_sql(table_name, conn, if_exists="append", index=False)
        conn.commit()
        logger.info("Saved %d rows to table '%s'", len(df), table_name)
    except Exception as exc:
        logger.error("save_raw failed for table '%s': %s", table_name, exc)
        raise
    finally:
        conn.close()


def log_pipeline_run(
    run_id: str,
    started_at: str,
    finished_at: str,
    rows_fetched: int,
    rows_saved: int,
    status: str,
    error: str,
) -> None:
    """Insert one pipeline execution record into pipeline_log."""
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO pipeline_log
                (run_id, started_at, finished_at, rows_fetched, rows_saved, status, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, started_at, finished_at, rows_fetched, rows_saved, status, error),
        )
        conn.commit()
        logger.info("Pipeline run %s logged with status=%s", run_id, status)
    except Exception as exc:
        logger.error("log_pipeline_run failed: %s", exc)
    finally:
        conn.close()
