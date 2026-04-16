"""
run_pipeline.py — CLI entry point for the Weather Analytics Pipeline.

Usage:
  python run_pipeline.py --once           Run the pipeline once and exit
  python run_pipeline.py --schedule       Start the APScheduler loop
  python run_pipeline.py --load-csv PATH  Load a historical CSV into SQLite
"""

import argparse
import logging
import os
import sys

# ── Logging setup (before any local imports) ──────────────────────────────────
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ── Ensure project root is on sys.path ────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from storage.db_writer import initialize_db


def main() -> None:
    """Parse CLI arguments and dispatch to the appropriate pipeline mode."""
    parser = argparse.ArgumentParser(
        description="Real-Time Weather Analytics Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py --once
  python run_pipeline.py --schedule
  python run_pipeline.py --load-csv data/historical.csv
        """,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--once",
        action="store_true",
        help="Run the full pipeline one time and exit",
    )
    group.add_argument(
        "--schedule",
        action="store_true",
        help="Start the hourly scheduler loop (blocks until Ctrl+C)",
    )
    group.add_argument(
        "--load-csv",
        metavar="CSV_PATH",
        help="Load a historical weather CSV into SQLite (runs batch_loader only)",
    )

    args = parser.parse_args()

    # ── Always initialise DB first ─────────────────────────────────────────────
    print("Initialising database …")
    initialize_db()
    print("Database ready.")

    if args.once:
        print("Running pipeline once …")
        from scheduler.pipeline_scheduler import run_full_pipeline
        run_full_pipeline()
        print("Pipeline complete.")

    elif args.schedule:
        print(f"Starting scheduler (interval = every N minutes, see config.py) …")
        from scheduler.pipeline_scheduler import start_scheduler
        start_scheduler()

    elif args.load_csv:
        csv_path: str = args.load_csv
        print(f"Loading historical CSV: {csv_path}")
        from ingestion.batch_loader import load_historical_csv
        load_historical_csv(csv_path)
        print("Batch load complete.")


if __name__ == "__main__":
    main()
