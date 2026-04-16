"""
ingestion/api_fetcher.py — Fetches live weather data from OpenWeatherMap API.

Provides functions to retrieve current conditions and 5-day forecasts,
parse the responses into flat records, and orchestrate full ingestion runs.
"""

import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from storage import db_writer

logger = logging.getLogger(__name__)

# ── Low-level API calls ───────────────────────────────────────────────────────

def fetch_current_weather(lat: float, lon: float) -> Optional[Dict]:
    """Call /weather endpoint and return raw JSON, or None on failure."""
    url = f"{config.BASE_URL}/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "units": "metric",
        "appid": config.OPENWEATHER_API_KEY,
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as exc:
        logger.error("fetch_current_weather failed (lat=%s, lon=%s): %s", lat, lon, exc)
        return None


def fetch_forecast(lat: float, lon: float, days: int = 5) -> Optional[Dict]:
    """Call /forecast endpoint and return 5-day 3-hourly JSON, or None on failure."""
    url = f"{config.BASE_URL}/forecast"
    params = {
        "lat": lat,
        "lon": lon,
        "units": "metric",
        "cnt": days * 8,          # 8 × 3-hour slots per day
        "appid": config.OPENWEATHER_API_KEY,
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as exc:
        logger.error("fetch_forecast failed (lat=%s, lon=%s): %s", lat, lon, exc)
        return None


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_current(raw: Dict, city_name: str) -> Dict:
    """Flatten a /weather JSON response into a clean flat dict."""
    fetched_at = datetime.now(timezone.utc).isoformat()
    sys_data = raw.get("sys", {})
    wind = raw.get("wind", {})
    clouds = raw.get("clouds", {})
    weather_list = raw.get("weather", [{}])
    main = raw.get("main", {})

    def _ts(epoch: Optional[int]) -> Optional[str]:
        if epoch is None:
            return None
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

    return {
        "city":          city_name,
        "country":       sys_data.get("country"),
        "timestamp":     _ts(raw.get("dt")),
        "fetched_at":    fetched_at,
        "temp_c":        main.get("temp"),
        "feels_like_c":  main.get("feels_like"),
        "temp_min_c":    main.get("temp_min"),
        "temp_max_c":    main.get("temp_max"),
        "humidity_pct":  main.get("humidity"),
        "pressure_hpa":  main.get("pressure"),
        "wind_speed_ms": wind.get("speed"),
        "wind_deg":      wind.get("deg"),
        "visibility_m":  raw.get("visibility"),
        "cloud_pct":     clouds.get("all"),
        "weather_main":  weather_list[0].get("main"),
        "weather_desc":  weather_list[0].get("description"),
        "sunrise_utc":   _ts(sys_data.get("sunrise")),
        "sunset_utc":    _ts(sys_data.get("sunset")),
    }


def parse_forecast(raw: Dict, city_name: str) -> List[Dict]:
    """Flatten a /forecast JSON response into a list of flat record dicts."""
    records: List[Dict] = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for item in raw.get("list", []):
        main = item.get("main", {})
        wind = item.get("wind", {})
        clouds = item.get("clouds", {})
        weather_list = item.get("weather", [{}])
        rain = item.get("rain", {})

        records.append({
            "city":          city_name,
            "forecast_time": item.get("dt_txt"),
            "fetched_at":    fetched_at,
            "temp_c":        main.get("temp"),
            "humidity_pct":  main.get("humidity"),
            "wind_speed_ms": wind.get("speed"),
            "cloud_pct":     clouds.get("all"),
            "weather_main":  weather_list[0].get("main"),
            "weather_desc":  weather_list[0].get("description"),
            "rain_3h_mm":    rain.get("3h", 0.0),
        })

    return records


# ── Orchestration ─────────────────────────────────────────────────────────────

def run_ingestion() -> int:
    """
    Loop over all cities, fetch current + forecast data, persist to DB and Parquet.

    Returns total number of raw current records saved.
    """
    os.makedirs(config.PARQUET_DIR, exist_ok=True)
    total_saved = 0
    timestamp_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    current_records: List[Dict] = []
    forecast_records: List[Dict] = []

    for city in config.CITIES:
        name = city["name"]
        lat, lon = city["lat"], city["lon"]

        # Current weather
        raw_current = fetch_current_weather(lat, lon)
        if raw_current:
            record = parse_current(raw_current, name)
            current_records.append(record)
            logger.info("Fetched current weather for %s: %.1f°C", name, record.get("temp_c", 0))
        else:
            logger.warning("Skipping current weather for %s — API returned None", name)

        # Forecast
        raw_forecast = fetch_forecast(lat, lon)
        if raw_forecast:
            fc_records = parse_forecast(raw_forecast, name)
            forecast_records.extend(fc_records)
            logger.info("Fetched %d forecast slots for %s", len(fc_records), name)
        else:
            logger.warning("Skipping forecast for %s — API returned None", name)

    # ── Persist to SQLite ────────────────────────────────────────────────────
    if current_records:
        db_writer.save_raw(current_records, "raw_weather")
        total_saved = len(current_records)

    # ── Persist to Parquet ───────────────────────────────────────────────────
    if current_records:
        df_curr = pd.DataFrame(current_records)
        parquet_path = os.path.join(
            config.PARQUET_DIR, f"current_{timestamp_tag}.parquet"
        )
        df_curr.to_parquet(parquet_path, index=False)
        logger.info("Saved current weather Parquet -> %s", parquet_path)

    if forecast_records:
        df_fc = pd.DataFrame(forecast_records)
        parquet_path = os.path.join(
            config.PARQUET_DIR, f"forecast_{timestamp_tag}.parquet"
        )
        df_fc.to_parquet(parquet_path, index=False)
        logger.info("Saved forecast Parquet -> %s", parquet_path)

    logger.info(
        "Ingestion complete -- %d current records, %d forecast slots",
        total_saved,
        len(forecast_records),
    )
    return total_saved
