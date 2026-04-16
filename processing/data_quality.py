"""
processing/data_quality.py — Data quality checks and audit logging.

Evaluates a weather DataFrame for completeness, duplicates, and physical
impossibilities. Persists the quality report to the quality_log SQLite table.
"""

import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Dict

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

logger = logging.getLogger(__name__)

# Columns to inspect for null counts
_CRITICAL_COLUMNS = [
    "city", "timestamp", "temp_c", "humidity_pct",
    "wind_speed_ms", "pressure_hpa",
]


def run_quality_checks(df: pd.DataFrame) -> Dict:
    """
    Run data quality checks on a weather DataFrame.

    Returns a report dict with keys:
        total_rows, null_counts, duplicate_rows, temp_anomalies,
        humidity_anomalies, quality_score, passed
    """
    report: Dict = {}

    total_rows = len(df)
    report["total_rows"] = total_rows

    if total_rows == 0:
        report.update({
            "null_counts": {},
            "duplicate_rows": 0,
            "temp_anomalies": 0,
            "humidity_anomalies": 0,
            "quality_score": 0.0,
            "passed": False,
        })
        logger.warning("Quality check received empty DataFrame")
        return report

    # ── Null counts per column ────────────────────────────────────────────────
    null_counts: Dict[str, int] = {}
    for col in df.columns:
        n = int(df[col].isna().sum())
        if n > 0:
            null_counts[col] = n
    report["null_counts"] = null_counts
    total_nulls = sum(null_counts.values())

    # ── Duplicate rows ────────────────────────────────────────────────────────
    duplicate_rows = int(df.duplicated().sum())
    report["duplicate_rows"] = duplicate_rows

    # ── Temperature anomalies (physically impossible values) ──────────────────
    if "temp_c" in df.columns:
        temp_series = pd.to_numeric(df["temp_c"], errors="coerce")
        temp_anomalies = int(((temp_series < -20) | (temp_series > 60)).sum())
    else:
        temp_anomalies = 0
    report["temp_anomalies"] = temp_anomalies

    # ── Humidity anomalies ────────────────────────────────────────────────────
    if "humidity_pct" in df.columns:
        hum_series = pd.to_numeric(df["humidity_pct"], errors="coerce")
        humidity_anomalies = int(((hum_series < 0) | (hum_series > 100)).sum())
    else:
        humidity_anomalies = 0
    report["humidity_anomalies"] = humidity_anomalies

    # ── Quality score (0-100) ─────────────────────────────────────────────────
    # Start at 100 and deduct proportional penalties
    score = 100.0

    # Deduct for nulls in critical columns (up to 30 points)
    critical_nulls = sum(
        int(df[c].isna().sum()) for c in _CRITICAL_COLUMNS if c in df.columns
    )
    critical_null_rate = critical_nulls / (total_rows * len(_CRITICAL_COLUMNS))
    score -= min(30.0, critical_null_rate * 100)

    # Deduct for duplicate rows (up to 20 points)
    dup_rate = duplicate_rows / total_rows
    score -= min(20.0, dup_rate * 100)

    # Deduct for temperature anomalies (up to 30 points)
    temp_anomaly_rate = temp_anomalies / total_rows
    score -= min(30.0, temp_anomaly_rate * 100)

    # Deduct for humidity anomalies (up to 20 points)
    hum_anomaly_rate = humidity_anomalies / total_rows
    score -= min(20.0, hum_anomaly_rate * 100)

    quality_score = round(max(0.0, score), 2)
    report["quality_score"] = quality_score
    report["passed"] = quality_score >= 80.0

    logger.info(
        "Quality check complete — rows=%d, nulls=%d, dups=%d, temp_anomalies=%d, "
        "hum_anomalies=%d, score=%.1f, passed=%s",
        total_rows,
        total_nulls,
        duplicate_rows,
        temp_anomalies,
        humidity_anomalies,
        quality_score,
        report["passed"],
    )
    return report


def log_quality_report(report: Dict, run_id: str) -> None:
    """Save a quality report dict as a row in the quality_log SQLite table."""
    timestamp = datetime.now(timezone.utc).isoformat()
    null_count_total = sum(report.get("null_counts", {}).values())
    anomaly_count = report.get("temp_anomalies", 0) + report.get("humidity_anomalies", 0)

    try:
        os.makedirs(os.path.dirname(config.DB_PATH) or ".", exist_ok=True)
        conn = sqlite3.connect(config.DB_PATH)
        conn.execute(
            """
            INSERT INTO quality_log
                (run_id, timestamp, total_rows, null_count_total,
                 duplicate_rows, anomaly_count, quality_score, passed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                timestamp,
                report.get("total_rows", 0),
                null_count_total,
                report.get("duplicate_rows", 0),
                anomaly_count,
                report.get("quality_score", 0.0),
                int(bool(report.get("passed", False))),
            ),
        )
        conn.commit()
        conn.close()
        logger.info("Quality report for run_id=%s saved to quality_log", run_id)
    except Exception as exc:
        logger.error("log_quality_report failed for run_id=%s: %s", run_id, exc)
