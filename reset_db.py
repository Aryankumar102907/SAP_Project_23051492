import sqlite3
import os

DB = "data/weather.db"
if os.path.exists(DB):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("DELETE FROM pipeline_log")
    c.execute("DELETE FROM quality_log")
    c.execute("DELETE FROM fact_weather")
    c.execute("DELETE FROM dim_condition")
    try:
        for tbl in ("pipeline_log", "quality_log", "fact_weather", "dim_condition"):
            c.execute(f"DELETE FROM sqlite_sequence WHERE name='{tbl}'")
    except Exception:
        pass
    conn.commit()
    fw = c.execute("SELECT COUNT(*) FROM fact_weather").fetchone()[0]
    pl = c.execute("SELECT COUNT(*) FROM pipeline_log").fetchone()[0]
    conn.close()
    print(f"Reset complete. fact_weather={fw} rows, pipeline_log={pl} rows")
else:
    print("DB not found")
