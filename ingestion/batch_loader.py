"""
ingestion/batch_loader.py — Loads historical weather data from CSV files into SQLite.

Handles common Kaggle weather dataset column naming variations and basic
data cleaning before persisting to the historical_weather table.
"""

import logging
import os
import sys
from typing import Optional

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from storage import db_writer

logger = logging.getLogger(__name__)

# Mapping of common Kaggle / external CSV column variants → our canonical names
_COLUMN_ALIASES: dict[str, str] = {
    # timestamp variants
    "date_time":       "timestamp",
    "datetime":        "timestamp",
    "date":            "timestamp",
    "time":            "timestamp",
    "dt":              "timestamp",
    "observation_time":"timestamp",
    # temperature variants
    "temperature":     "temp_c",
    "temp":            "temp_c",
    "temperature_c":   "temp_c",
    "maxtempC":        "temp_max_c",
    "mintempC":        "temp_min_c",
    # humidity variants
    "humidity":        "humidity_pct",
    "relative_humidity":"humidity_pct",
    # wind variants
    "windspeed":       "wind_speed_ms",
    "wind_speed":      "wind_speed_ms",
    "wind_kph":        "wind_speed_ms",
    # weather description variants
    "weather_description": "weather_desc",
    "description":         "weather_desc",
    "weatherCode":         "weather_main",
    "weatherdesc":         "weather_desc",
    # city variants
    "location":        "city",
    "station":         "city",
}


def load_historical_csv(csv_path: str) -> None:
    """
    Read a weather CSV, normalise columns, clean data, and persist to historical_weather.

    Handles common Kaggle dataset column naming variants.
    Drops rows with null timestamp or temp_c.
    Converts humidity from 0-1 scale to 0-100 if all values are < 1.0.
    """
    if not os.path.isfile(csv_path):
        logger.error("CSV file not found: %s", csv_path)
        return

    try:
        df = pd.read_csv(csv_path)
        logger.info("Loaded %d rows from %s", len(df), csv_path)
    except Exception as exc:
        logger.error("Failed to read CSV '%s': %s", csv_path, exc)
        return

    # ── Normalise column names ────────────────────────────────────────────────
    df.columns = [c.strip().lower() for c in df.columns]
    rename_map = {k.lower(): v for k, v in _COLUMN_ALIASES.items()}
    df = df.rename(columns=rename_map)

    # ── Ensure required columns exist ─────────────────────────────────────────
    for col in ("timestamp", "temp_c"):
        if col not in df.columns:
            logger.error("Required column '%s' not found in CSV after renaming. "
                         "Available columns: %s", col, list(df.columns))
            return

    # ── Drop rows with null timestamp or temp_c ───────────────────────────────
    before = len(df)
    df = df.dropna(subset=["timestamp", "temp_c"])
    dropped = before - len(df)
    if dropped:
        logger.info("Dropped %d rows with null timestamp or temp_c", dropped)

    # ── Fix humidity scale (0-1 → 0-100) ─────────────────────────────────────
    if "humidity_pct" in df.columns:
        try:
            numeric_humidity = pd.to_numeric(df["humidity_pct"], errors="coerce")
            if numeric_humidity.dropna().lt(1.0).all():
                df["humidity_pct"] = numeric_humidity * 100
                logger.info("Converted humidity_pct from 0-1 scale to 0-100")
        except Exception as exc:
            logger.warning("Humidity scale check failed: %s", exc)

    # ── Persist to SQLite ─────────────────────────────────────────────────────
    try:
        import sqlite3
        os.makedirs(os.path.dirname(config.DB_PATH) or ".", exist_ok=True)
        conn = sqlite3.connect(config.DB_PATH)
        df.to_sql("historical_weather", conn, if_exists="replace", index=False)
        conn.commit()
        conn.close()
        logger.info("Saved %d rows to historical_weather table", len(df))
    except Exception as exc:
        logger.error("Failed to save historical CSV to SQLite: %s", exc)
        raise
