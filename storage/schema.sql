-- ============================================================
-- schema.sql  —  Star schema for the Weather Analytics Pipeline
-- ============================================================

-- Dimension: Cities
CREATE TABLE IF NOT EXISTS dim_city (
    city_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    city_name TEXT    NOT NULL UNIQUE,
    country   TEXT,
    latitude  REAL,
    longitude REAL
);

-- Dimension: Calendar dates
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    full_date   TEXT NOT NULL UNIQUE,
    year        INTEGER,
    month       INTEGER,
    day         INTEGER,
    day_of_week TEXT,
    is_weekend  INTEGER   -- 0 = weekday, 1 = weekend
);

-- Dimension: Weather conditions
CREATE TABLE IF NOT EXISTS dim_condition (
    condition_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    weather_main  TEXT,
    weather_desc  TEXT,
    comfort_level TEXT,
    UNIQUE(weather_main, weather_desc, comfort_level)
);

-- Fact: Hourly weather observations (star schema centre)
CREATE TABLE IF NOT EXISTS fact_weather (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    city_id       INTEGER REFERENCES dim_city(city_id),
    date_id       INTEGER REFERENCES dim_date(date_id),
    condition_id  INTEGER REFERENCES dim_condition(condition_id),
    timestamp     TEXT,
    temp_c        REAL,
    feels_like_c  REAL,
    temp_min_c    REAL,
    temp_max_c    REAL,
    humidity_pct  REAL,
    pressure_hpa  REAL,
    wind_speed_ms REAL,
    wind_deg      REAL,
    visibility_m  REAL,
    cloud_pct     REAL,
    heat_index_c  REAL,
    wind_chill_c  REAL,
    is_daytime    INTEGER,
    fetched_at    TEXT
);

-- Raw ingested records (no schema enforcement)
CREATE TABLE IF NOT EXISTS raw_weather (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    city          TEXT,
    country       TEXT,
    timestamp     TEXT,
    fetched_at    TEXT,
    temp_c        REAL,
    feels_like_c  REAL,
    temp_min_c    REAL,
    temp_max_c    REAL,
    humidity_pct  REAL,
    pressure_hpa  REAL,
    wind_speed_ms REAL,
    wind_deg      REAL,
    visibility_m  REAL,
    cloud_pct     REAL,
    weather_main  TEXT,
    weather_desc  TEXT,
    sunrise_utc   TEXT,
    sunset_utc    TEXT
);

-- Historical weather loaded from CSV
CREATE TABLE IF NOT EXISTS historical_weather (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    city          TEXT,
    timestamp     TEXT,
    temp_c        REAL,
    humidity_pct  REAL,
    wind_speed_ms REAL,
    weather_main  TEXT,
    weather_desc  TEXT
);

-- Data quality audit log
CREATE TABLE IF NOT EXISTS quality_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id           TEXT,
    timestamp        TEXT,
    total_rows       INTEGER,
    null_count_total INTEGER,
    duplicate_rows   INTEGER,
    anomaly_count    INTEGER,
    quality_score    REAL,
    passed           INTEGER
);

-- Pipeline execution log
CREATE TABLE IF NOT EXISTS pipeline_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT,
    started_at  TEXT,
    finished_at TEXT,
    rows_fetched INTEGER,
    rows_saved   INTEGER,
    status      TEXT,
    error       TEXT
);
