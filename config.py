"""
config.py — Central configuration for the Weather Analytics Pipeline.
Loads environment variables and defines shared constants used across modules.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── API ──────────────────────────────────────────────────────────────────────
OPENWEATHER_API_KEY: str = os.getenv("OPENWEATHER_API_KEY", "")
BASE_URL: str = "http://api.openweathermap.org/data/2.5"

# ── Cities — 10 Key Indian Metro Cities ──────────────────────────────────────
CITIES: list[dict] = [
    {"name": "Delhi",     "lat": 28.6139, "lon": 77.2090},
    {"name": "Mumbai",    "lat": 19.0760, "lon": 72.8777},
    {"name": "Kolkata",   "lat": 22.5726, "lon": 88.3639},
    {"name": "Chennai",   "lat": 13.0827, "lon": 80.2707},
    {"name": "Bangalore", "lat": 12.9716, "lon": 77.5946},
    {"name": "Hyderabad", "lat": 17.3850, "lon": 78.4867},
    {"name": "Ahmedabad", "lat": 23.0225, "lon": 72.5714},
    {"name": "Pune",      "lat": 18.5204, "lon": 73.8567},
    {"name": "Surat",     "lat": 21.1702, "lon": 72.8311},
    {"name": "Jaipur",    "lat": 26.9124, "lon": 75.7873},
]

# ── Storage ──────────────────────────────────────────────────────────────────
DB_PATH: str = "data/weather.db"
PARQUET_DIR: str = "data/parquet/"
LOG_FILE: str = "logs/pipeline.log"

# ── Scheduler ────────────────────────────────────────────────────────────────
FETCH_INTERVAL_MINUTES: int = 15
