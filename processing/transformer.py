"""
processing/transformer.py â€” Cleans and enriches raw weather DataFrames.

Adds meteorological derived metrics (heat index, wind chill, comfort level)
and temporal features. Also provides daily aggregation utilities.
"""

import logging
import os
import sys
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)


# â”€â”€ Meteorological formulae â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _heat_index(temp_c: float, humidity_pct: float) -> float:
    """
    Steadman heat index formula (Â°C).

    Valid only when temp_c >= 27 and humidity_pct >= 40;
    returns temp_c unchanged otherwise.
    """
    if temp_c < 27 or humidity_pct < 40:
        return temp_c

    T = temp_c
    RH = humidity_pct
    HI = (
        -8.78469475556
        + 1.61139411 * T
        + 2.33854883889 * RH
        - 0.14611605 * T * RH
        - 0.012308094 * T ** 2
        - 0.016424828 * RH ** 2
        + 0.002211732 * T ** 2 * RH
        + 0.00072546 * T * RH ** 2
        - 0.000003582 * T ** 2 * RH ** 2
    )
    return round(HI, 2)


def _wind_chill(temp_c: float, wind_speed_ms: float) -> float:
    """
    Environment Canada wind chill formula (Â°C).

    Valid only when temp_c <= 10 and wind_speed_ms >= 1.3;
    returns temp_c unchanged otherwise.
    """
    if temp_c > 10 or wind_speed_ms < 1.3:
        return temp_c

    # Convert m/s â†’ km/h for the formula
    wind_kmh = wind_speed_ms * 3.6
    WC = (
        13.12
        + 0.6215 * temp_c
        - 11.37 * wind_kmh ** 0.16
        + 0.3965 * temp_c * wind_kmh ** 0.16
    )
    return round(WC, 2)


def _comfort(heat_index_c: float, temp_c: float) -> str:
    """Classify comfort level based on heat index and raw temperature."""
    if heat_index_c > 35:
        return "Hot"
    elif temp_c >= 27:
        return "Warm"
    elif temp_c >= 18:
        return "Pleasant"
    else:
        return "Cold"


# â”€â”€ Main transform â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def transform_raw_weather(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich a raw_weather DataFrame with derived meteorological columns.

    Adds: heat_index_c, wind_chill_c, comfort_level, hour, day_of_week,
    is_daytime.

    Returns the enriched DataFrame (original is not mutated).
    """
    if df.empty:
        logger.warning("transform_raw_weather received an empty DataFrame")
        return df

    df = df.copy()

    # â”€â”€ Parse timestamp â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    # â”€â”€ Ensure numeric columns â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    for col in ("temp_c", "humidity_pct", "wind_speed_ms"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # â”€â”€ Heat index â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    df["heat_index_c"] = df.apply(
        lambda r: _heat_index(
            r.get("temp_c", 0) or 0,
            r.get("humidity_pct", 0) or 0,
        ),
        axis=1,
    )

    # â”€â”€ Wind chill â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    df["wind_chill_c"] = df.apply(
        lambda r: _wind_chill(
            r.get("temp_c", 0) or 0,
            r.get("wind_speed_ms", 0) or 0,
        ),
        axis=1,
    )

    # â”€â”€ Comfort level â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    df["comfort_level"] = df.apply(
        lambda r: _comfort(r["heat_index_c"], r.get("temp_c", 0) or 0),
        axis=1,
    )

    # â”€â”€ Temporal features â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    df["hour"]        = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.strftime("%A")

    # â”€â”€ is_daytime â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    def _is_daytime(row: pd.Series) -> bool:
        ts = row.get("timestamp")
        sr = row.get("sunrise_utc")
        ss = row.get("sunset_utc")
        if pd.isna(ts) or not sr or not ss:
            return True  # default to daytime if unknown
        try:
            sunrise = pd.to_datetime(sr, utc=True)
            sunset  = pd.to_datetime(ss, utc=True)
            return sunrise <= ts <= sunset
        except Exception:
            return True

    if "sunrise_utc" in df.columns and "sunset_utc" in df.columns:
        df["is_daytime"] = df.apply(_is_daytime, axis=1)
    else:
        df["is_daytime"] = True

    logger.info("Transformed %d rows â€” added heat_index, wind_chill, comfort_level", len(df))
    return df


# â”€â”€ Daily aggregation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def aggregate_daily_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group a transformed DataFrame by city + date and compute daily statistics.

    Returns a DataFrame with columns:
        city, date, avg_temp, max_temp, min_temp,
        avg_humidity, avg_wind_speed, dominant_weather
    """
    if df.empty:
        logger.warning("aggregate_daily_stats received an empty DataFrame")
        return df

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df["date"] = df["timestamp"].dt.date

    agg = (
        df.groupby(["city", "date"])
        .agg(
            avg_temp=("temp_c", "mean"),
            max_temp=("temp_c", "max"),
            min_temp=("temp_c", "min"),
            avg_humidity=("humidity_pct", "mean"),
            avg_wind_speed=("wind_speed_ms", "mean"),
            dominant_weather=("weather_main", lambda x: x.mode()[0] if not x.empty else None),
        )
        .reset_index()
    )

    for col in ("avg_temp", "max_temp", "min_temp", "avg_humidity", "avg_wind_speed"):
        agg[col] = agg[col].round(2)

    logger.info("Aggregated daily stats: %d city-date combinations", len(agg))
    return agg

