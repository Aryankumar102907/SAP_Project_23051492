"""
dashboard/app.py — Weather Analytics Pipeline Dashboard.

Design: Deep-sea precision instrument — cool aqua-to-lime gradient palette.
Typography: Outfit (UI) + JetBrains Mono (data).
Color palette: Deep ocean base, teal/cyan accents, lime-yellow highlights.
"""

import os
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# ─────────────────────────────────────────────────────────────────────────────
# Page configuration
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Weather Analytics Pipeline",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# Design system — Impeccable precision instrument theme
# ─────────────────────────────────────────────────────────────────────────────
STYLE = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@300;400;500;600&display=swap');

:root {
  --bg-base:      #000000;
  --bg-surface:   #0d0d1a;
  --bg-raised:    #15152a;
  --border-sub:   #2a2a4a;
  --border-mid:   #3d3d6a;
  --text-hi:      #CBCCFF;
  --text-mid:     #9296F0;
  --text-lo:      #5B63B7;
  --accent:       #9296F0;
  --accent2:      #CBCCFF;
  --city-col:     #9296F0;
  --ok:           #7e81f5;
  --warn:         #CBCCFF;
  --fail:         #ff6b8a;
}

html, body, [class*="css"] {
  font-family: 'Outfit', system-ui, sans-serif !important;
  background-color: var(--bg-base) !important;
  color: var(--text-hi);
}

section[data-testid="stSidebar"] {
  background: #000000 !important;
  border-right: 1px solid var(--border-sub) !important;
}
section[data-testid="stSidebar"] > div { padding-top: 8px; }

.main .block-container {
  padding: 32px 40px 64px !important;
  max-width: 1400px;
}

h1 {
  font-family: 'Outfit', sans-serif !important;
  font-size: 1.4rem !important;
  font-weight: 800 !important;
  letter-spacing: -0.02em !important;
  color: var(--accent2) !important;
  margin-bottom: 4px !important;
}
h2, h3 {
  font-family: 'Outfit', sans-serif !important;
  font-size: 0.68rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.12em !important;
  text-transform: uppercase !important;
  color: var(--text-lo) !important;
  margin-top: 32px !important;
  margin-bottom: 12px !important;
}

div[data-testid="metric-container"] {
  background: var(--bg-surface) !important;
  border: 1px solid var(--border-sub) !important;
  border-top: 2px solid var(--accent) !important;
  border-radius: 6px !important;
  padding: 20px !important;
  box-shadow: 0 2px 16px rgba(146,150,240,0.12) !important;
}
div[data-testid="metric-container"] label {
  font-family: 'Outfit', sans-serif !important;
  font-size: 0.65rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.1em !important;
  text-transform: uppercase !important;
  color: var(--text-lo) !important;
}
div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
  font-family: 'JetBrains Mono', monospace !important;
  font-size: 1.6rem !important;
  font-weight: 600 !important;
  color: var(--accent2) !important;
  letter-spacing: -0.02em !important;
}

div[data-testid="stRadio"] label {
  font-family: 'Outfit', sans-serif !important;
  font-size: 0.875rem !important;
  color: var(--text-mid) !important;
}
div[data-testid="stRadio"] [aria-checked="true"] label {
  color: var(--accent2) !important;
  font-weight: 700 !important;
}

.dataframe { font-family: 'JetBrains Mono', monospace !important; font-size: 0.78rem !important; color: var(--text-hi) !important; }
.dataframe th {
  background: var(--bg-raised) !important;
  color: var(--accent2) !important;
  font-family: 'Outfit', sans-serif !important;
  font-size: 0.65rem !important;
  letter-spacing: 0.08em !important;
  text-transform: uppercase !important;
  font-weight: 700 !important;
}
.dataframe td { background: var(--bg-surface) !important; color: var(--text-mid) !important; }

div[data-testid="stAlert"] {
  border-radius: 6px !important;
  font-family: 'Outfit', sans-serif !important;
  font-size: 0.875rem !important;
  background: var(--bg-raised) !important;
}
div[data-testid="stCaption"] {
  font-family: 'JetBrains Mono', monospace !important;
  font-size: 0.72rem !important;
  color: var(--text-lo) !important;
}
div[data-testid="stSelectbox"] label,
div[data-testid="stMultiSelect"] label,
div[data-testid="stDateInput"] label {
  font-family: 'Outfit', sans-serif !important;
  font-size: 0.75rem !important;
  font-weight: 700 !important;
  color: var(--text-lo) !important;
  letter-spacing: 0.08em !important;
  text-transform: uppercase !important;
}
hr { border-color: var(--border-sub) !important; }
</style>
"""
st.markdown(STYLE, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────
_DB = config.DB_PATH


@st.cache_data(ttl=60, show_spinner=False)
def _query(sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        if not os.path.exists(_DB):
            return pd.DataFrame()
        conn = sqlite3.connect(_DB)
        df = pd.read_sql_query(sql, conn, params=params)
        conn.close()
        return df
    except Exception as exc:
        st.error(f"Database error: {exc}")
        return pd.DataFrame()


def _no_data_msg() -> None:
    st.info("No data yet. Run `python run_pipeline.py --once` then refresh.")


# ─────────────────────────────────────────────────────────────────────────────
# Palette — Skip Gradient: #000000 → #CBCCFF → #9296F0 → #5B63B7
# ─────────────────────────────────────────────────────────────────────────────
CITY_COLORS = {
    "Delhi":     "#CBCCFF",  # Lavender
    "Mumbai":    "#9296F0",  # Medium Purple
    "Kolkata":   "#5B63B7",  # Deep Blue
    "Chennai":   "#a8aaff",  # Soft Lavender
    "Bangalore": "#7880e8",  # Blue-Purple
    "Hyderabad": "#d4d5ff",  # Pale Lavender
    "Ahmedabad": "#6b72d0",  # Muted Purple
    "Pune":      "#b0b2f8",  # Light Purple
    "Surat":     "#878ae0",  # Mid Purple
    "Jaipur":    "#4a5299",  # Dark Blue
}

STATUS_COLORS = {
    "Hot":      "#ff6b8a",
    "Warm":     "#9296F0",
    "Pleasant": "#7880e8",
    "Cold":     "#5B63B7",
}
ACCENT       = "#9296F0"
TEXT_MID     = "#9296F0"
TEXT_LO      = "#5B63B7"
SURFACE_RGBA = "rgba(13,13,26,0.97)"
FONT_DATA    = "JetBrains Mono"
FONT_UI      = "Outfit"

def get_ax(figsize=(12, 4.5), xlabel=None, ylabel=None):
    """Skip Gradient palette: black base, lavender/purple labels — fully visible."""
    fig, ax = plt.subplots(figsize=figsize)
    fig.patch.set_facecolor('#000000')
    ax.set_facecolor('#0d0d1a')
    ax.grid(color='#1e1e3a', linestyle='--', linewidth=0.7, zorder=0)
    for spine in ['top', 'right']:
        ax.spines[spine].set_visible(False)
    for spine in ['left', 'bottom']:
        ax.spines[spine].set_color('#5B63B7')
    ax.tick_params(colors='#CBCCFF', labelsize=9, which='both')
    ax.xaxis.label.set_color('#CBCCFF')
    ax.yaxis.label.set_color('#CBCCFF')
    if xlabel:
        ax.set_xlabel(xlabel, color='#CBCCFF', fontsize=10, fontweight='bold', labelpad=8)
    if ylabel:
        ax.set_ylabel(ylabel, color='#CBCCFF', fontsize=10, fontweight='bold', labelpad=8)
    return fig, ax


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""<div style="padding:8px 16px 20px;">
        <div style="font-family:'JetBrains Mono',monospace;font-size:0.6rem;
                    letter-spacing:0.16em;text-transform:uppercase;
                    color:#2a2a4a;margin-bottom:4px;">PIPELINE</div>
        <div style="font-family:'Outfit',sans-serif;font-size:1.1rem;
                    font-weight:800;color:#CBCCFF;letter-spacing:-0.01em;
                    line-height:1.2;">Weather Analytics</div>
        <div style="font-family:'Outfit',sans-serif;font-size:0.78rem;
                    color:#5B63B7;margin-top:2px;">
            10 Major Metro Hubs</div>
    </div>""", unsafe_allow_html=True)

    st.markdown("""<div style="height:1px;background:#2a2a4a;margin:0 16px 16px;"></div>""",
                unsafe_allow_html=True)

    page = st.radio(
        "Navigation",
        ["Live Conditions", "Historical Trends", "City Comparison", "Pipeline Health"],
        label_visibility="collapsed",
    )

    st.markdown("""<div style="height:1px;background:#2a2a4a;margin:16px 16px 12px;"></div>""",
                unsafe_allow_html=True)
    st.markdown(
        f"""<div style="font-family:'JetBrains Mono',monospace;font-size:0.65rem;
                    color:#5B63B7;padding:0 16px;line-height:2;">
            Refresh interval<br>
            <span style="color:#CBCCFF;">{config.FETCH_INTERVAL_MINUTES} min</span>
        </div>""",
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 1 — Live Conditions
# ─────────────────────────────────────────────────────────────────────────────
if page == "Live Conditions":
    st.title("Live Conditions")

    df = _query("""
        SELECT dc.city_name AS city, fw.timestamp, fw.temp_c, fw.feels_like_c,
               fw.humidity_pct, fw.wind_speed_ms, fw.cloud_pct, fw.pressure_hpa,
               fw.heat_index_c, fw.wind_chill_c, fw.is_daytime,
               dco.weather_desc, dco.weather_main, dco.comfort_level, fw.fetched_at
        FROM fact_weather fw
        JOIN dim_city      dc  ON fw.city_id     = dc.city_id
        JOIN dim_condition dco ON fw.condition_id = dco.condition_id
        WHERE fw.id IN (SELECT MAX(id) FROM fact_weather GROUP BY city_id)
        ORDER BY dc.city_name
    """)

    if df.empty:
        _no_data_msg()
    else:
        try:
            fetched_dt = pd.to_datetime(df["fetched_at"].iloc[0])
            fetched_clean = f"Last updated: {fetched_dt.strftime('%d %b %Y, %H:%M')} UTC"
        except Exception:
            fetched_clean = "Last updated: Unknown"
            
        st.caption(fetched_clean)
        cols = st.columns(3, gap="medium")

        for idx, row in df.iterrows():
            city     = row["city"]
            comfort  = row.get("comfort_level", "-")
            city_c   = CITY_COLORS.get(city, "#8899b4")
            
            bg_colors = {"Hot": "#ff6b8a", "Warm": "#9296F0", "Pleasant": "#7880e8", "Cold": "#5B63B7"}
            badge_bg = bg_colors.get(comfort, "#3d3d6a")
            
            period   = "☀️ Day" if row.get("is_daytime") else "🌙 Night"
            
            w_main = row.get("weather_main", "")
            if w_main == "Clear":        w_icon = "☀️"
            elif w_main == "Clouds":     w_icon = "⛅"
            elif w_main == "Rain":       w_icon = "🌧️"
            elif w_main == "Haze":       w_icon = "🌫️"
            elif w_main == "Thunderstorm": w_icon = "⛈️"
            elif w_main == "Snow":       w_icon = "❄️"
            elif w_main == "Drizzle":    w_icon = "🌦️"
            else:                        w_icon = "🌡️"

            with cols[idx % 3]:
                card_html = f"""
<div style="background:#0d0d1a;border:1px solid #2a2a4a;border-top:3px solid {city_c};border-radius:8px;padding:18px 18px 14px;margin-bottom:12px;box-shadow:0 4px 20px rgba(146,150,240,0.12);">
    <div style="display:flex; justify-content:space-between; margin-bottom:12px;">
        <span style="font-family:'Outfit',sans-serif;font-size:0.7rem;font-weight:800;letter-spacing:0.13em;text-transform:uppercase;color:{city_c};">{city}</span>
        <span style="font-family:'JetBrains Mono',monospace;font-size:0.65rem;color:#5B63B7;">{period}</span>
    </div>
    <div style="font-family:'JetBrains Mono',monospace;font-size:2.5rem;font-weight:600;color:#CBCCFF;letter-spacing:-0.03em;line-height:1;margin:12px 0 3px;">{row['temp_c']:.1f}&deg;C</div>
    <div style="font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:#5B63B7;margin-bottom:8px;">feels {row['feels_like_c']:.1f}&deg;C</div>
    <div style="font-family:'Outfit',sans-serif;font-size:0.85rem;font-weight:500;color:#9296F0;text-transform:capitalize;margin-bottom:12px;">{w_icon} {row.get('weather_desc', '-')}</div>
    <div style="height:1px;background:#2a2a4a;margin-bottom:12px;"></div>
    <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 12px;">
        <div>
            <div style="font-family:'Outfit',sans-serif;font-size:0.72rem;font-weight:800;letter-spacing:0.05em;text-transform:uppercase;color:#5B63B7;">Humidity</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:0.9rem;color:#CBCCFF;">{row['humidity_pct']:.0f}%</div>
        </div>
        <div>
            <div style="font-family:'Outfit',sans-serif;font-size:0.72rem;font-weight:800;letter-spacing:0.05em;text-transform:uppercase;color:#5B63B7;">Wind</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:0.9rem;color:#CBCCFF;">{row['wind_speed_ms']:.1f} m/s</div>
        </div>
        <div>
            <div style="font-family:'Outfit',sans-serif;font-size:0.72rem;font-weight:800;letter-spacing:0.05em;text-transform:uppercase;color:#5B63B7;">Cloud</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:0.9rem;color:#CBCCFF;">{row['cloud_pct']:.0f}%</div>
        </div>
        <div>
            <div style="font-family:'Outfit',sans-serif;font-size:0.72rem;font-weight:800;letter-spacing:0.05em;text-transform:uppercase;color:#5B63B7;">Pressure</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:0.9rem;color:#CBCCFF;">{row['pressure_hpa']:.0f} hPa</div>
        </div>
    </div>
    <div style="margin-top:14px;display:inline-block;background:{badge_bg};color:#000000;padding:4px 10px;border-radius:4px;font-family:'Outfit',sans-serif;font-size:0.65rem;font-weight:800;letter-spacing:0.1em;text-transform:uppercase;">{comfort}</div>
</div>
"""
                st.markdown(card_html, unsafe_allow_html=True)

        time.sleep(60)
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 2 — Historical Trends
# ─────────────────────────────────────────────────────────────────────────────
elif page == "Historical Trends":
    st.title("Historical Trends")

    col_sel, col_date = st.columns([1, 2])
    with col_sel:
        city_list = ["All Cities"] + [c["name"] for c in config.CITIES]
        selected_city = st.selectbox("City", city_list, key="hist_city")
    with col_date:
        default_end   = datetime.now(timezone.utc).date()
        default_start = default_end - timedelta(days=7)
        date_range    = st.date_input(
            "Date range", value=(default_start, default_end), key="hist_dates",
        )

    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = default_start, default_end

    city_filter = "" if selected_city == "All Cities" \
        else f"AND dc.city_name = '{selected_city}'"

    df = _query(f"""
        SELECT dc.city_name AS city, fw.timestamp,
               fw.temp_c, fw.temp_min_c, fw.temp_max_c,
               fw.humidity_pct, fw.wind_speed_ms, dco.weather_main
        FROM fact_weather fw
        JOIN dim_city      dc  ON fw.city_id     = dc.city_id
        JOIN dim_condition dco ON fw.condition_id = dco.condition_id
        WHERE DATE(fw.timestamp) BETWEEN '{start_date}' AND '{end_date}'
        {city_filter}
        ORDER BY fw.timestamp
    """)

    if df.empty:
        _no_data_msg()
    else:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        # Filter to only cities tracked in current palette
        df = df[df["city"].isin(CITY_COLORS)]
        if df.empty:
            st.info("No data yet for currently tracked cities. Run the pipeline and refresh.")
            st.stop()
        n_cities = df["city"].nunique()

        st.subheader("Temperature")
        fig_temp, ax_temp = get_ax(figsize=(12, 4), ylabel="\u00b0C")
        for city in df["city"].unique():
            cdf   = df[df["city"] == city].sort_values("timestamp")
            color = CITY_COLORS.get(city, "#8a7d5a")
            if len(cdf) == 1:
                ax_temp.scatter(cdf["timestamp"], cdf["temp_c"], color=color, s=60, zorder=5, label=city)
            else:
                ax_temp.plot(cdf["timestamp"], cdf["temp_c"], label=city, color=color, linewidth=2, zorder=3)
                ax_temp.fill_between(cdf["timestamp"], cdf["temp_min_c"], cdf["temp_max_c"], color=color, alpha=0.10, zorder=2)
        ax_temp.legend(frameon=False, labelcolor="#FEFDDF", loc="upper left", fontsize=8,
                       ncol=2, bbox_to_anchor=(0, 1))
        fig_temp.tight_layout()
        st.pyplot(fig_temp, transparent=True)
        plt.close(fig_temp)

        st.subheader("Humidity")
        df["date"] = df["timestamp"].dt.date.astype(str)
        plot_palette_hum = {c: CITY_COLORS[c] for c in df["city"].unique() if c in CITY_COLORS}
        fig_hum, ax_hum = get_ax(figsize=(12, 3.5), ylabel="RH%")
        sns.barplot(data=df, x="date", y="humidity_pct", hue="city", palette=plot_palette_hum,
                    ax=ax_hum, errorbar=None, zorder=3)
        ax_hum.legend(frameon=False, labelcolor="#FEFDDF", loc="upper right", fontsize=8, ncol=2)
        fig_hum.tight_layout()
        st.pyplot(fig_hum, transparent=True)
        plt.close(fig_hum)

        st.subheader("Wind Speed")
        fig_wind, ax_wind = get_ax(figsize=(12, 4), ylabel="m/s")
        for city in df["city"].unique():
            cdf = df[df["city"] == city].sort_values("timestamp")
            color = CITY_COLORS.get(city, "#8a7d5a")
            if len(cdf) == 1:
                ax_wind.scatter(cdf["timestamp"], cdf["wind_speed_ms"], color=color, s=60, zorder=5, label=city)
            else:
                ax_wind.plot(cdf["timestamp"], cdf["wind_speed_ms"], label=city, color=color, linewidth=2, zorder=3)
        ax_wind.legend(frameon=False, labelcolor="#FEFDDF", loc="upper left", fontsize=8,
                       ncol=2, bbox_to_anchor=(0, 1))
        fig_wind.tight_layout()
        st.pyplot(fig_wind, transparent=True)
        plt.close(fig_wind)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 3 — City Comparison
# ─────────────────────────────────────────────────────────────────────────────
elif page == "City Comparison":
    st.title("City Comparison")
    
    city_list = [c["name"] for c in config.CITIES]
    valid_defaults = [c for c in ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai"] if c in city_list]
    sel_cmps = st.multiselect("Select Cities to Compare", city_list, default=valid_defaults)
    
    if not sel_cmps:
        st.warning("Please select at least one city to proceed.")
        st.stop()
        
    in_clause = ", ".join([f"'{c}'" for c in sel_cmps])

    df_daily = _query(f"""
        SELECT dc.city_name AS city, DATE(fw.timestamp) AS date,
               AVG(fw.temp_c)        AS avg_temp,
               MAX(fw.temp_c)        AS max_temp,
               MIN(fw.temp_c)        AS min_temp,
               AVG(fw.humidity_pct)  AS avg_humidity,
               AVG(fw.wind_speed_ms) AS avg_wind
        FROM fact_weather fw
        JOIN dim_city dc ON fw.city_id = dc.city_id
        WHERE dc.city_name IN ({in_clause})
        GROUP BY dc.city_name, DATE(fw.timestamp)
        ORDER BY date
    """)

    df_raw = _query(f"""
        SELECT dc.city_name AS city, fw.temp_c, fw.humidity_pct
        FROM fact_weather fw
        JOIN dim_city dc ON fw.city_id = dc.city_id
        WHERE dc.city_name IN ({in_clause})
    """)

    if df_daily.empty and df_raw.empty:
        _no_data_msg()
    else:
        # Filter to only known cities
        if not df_daily.empty:
            df_daily = df_daily[df_daily["city"].isin(CITY_COLORS)]
        if not df_raw.empty:
            df_raw = df_raw[df_raw["city"].isin(CITY_COLORS)]

        if not df_daily.empty:
            st.subheader("Average Temperature by Day")
            df_daily["date"] = df_daily["date"].astype(str)
            palette_daily = {c: CITY_COLORS[c] for c in df_daily["city"].unique() if c in CITY_COLORS}
            fig_bar, ax_bar = get_ax(ylabel="deg C")
            sns.barplot(data=df_daily, x="date", y="avg_temp", hue="city", palette=palette_daily, ax=ax_bar, zorder=3)
            if len(sel_cmps) <= 10:
                ax_bar.legend(frameon=False, labelcolor='#CBCCFF', loc='upper right', fontsize=9)
            else:
                ax_bar.get_legend().remove()
            st.pyplot(fig_bar, transparent=True)
            plt.close(fig_bar)

        if not df_raw.empty:
            st.subheader("Humidity vs Temperature")
            palette_raw = {c: CITY_COLORS[c] for c in df_raw["city"].unique() if c in CITY_COLORS}
            fig_scatter, ax_scatter = get_ax(xlabel="Temperature (C)", ylabel="Humidity (%)")
            sns.scatterplot(data=df_raw, x="temp_c", y="humidity_pct", hue="city", palette=palette_raw, ax=ax_scatter, s=70, alpha=0.9, edgecolor=None, zorder=3)
            if len(sel_cmps) <= 10:
                ax_scatter.legend(frameon=False, labelcolor='#CBCCFF')
            else:
                l = ax_scatter.get_legend()
                if l: l.remove()
            st.pyplot(fig_scatter, transparent=True)
            plt.close(fig_scatter)

        if not df_daily.empty:
            st.subheader("Summary Statistics")
            summary = (
                df_daily.groupby("city")
                .agg(
                    avg_temp=("avg_temp", "mean"),
                    max_temp=("max_temp", "max"),
                    min_temp=("min_temp", "min"),
                    avg_humidity=("avg_humidity", "mean"),
                    avg_wind=("avg_wind", "mean"),
                    days=("date", "count"),
                )
                .reset_index().round(1)
            )
            summary.columns = [
                "City", "Avg Temp (C)", "Max (C)", "Min (C)",
                "Avg Humidity (%)", "Avg Wind (m/s)", "Days",
            ]
            st.dataframe(summary, width="stretch", hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 4 — Pipeline Health
# ─────────────────────────────────────────────────────────────────────────────
elif page == "Pipeline Health":
    st.title("Pipeline Health")

    st.subheader("Data Quality Score")
    df_qual = _query(
        "SELECT run_id, timestamp, quality_score, passed FROM quality_log ORDER BY timestamp"
    )
    if not df_qual.empty:
        df_qual["timestamp"] = pd.to_datetime(df_qual["timestamp"], errors="coerce", utc=True)
        fig_qual, ax_qual = get_ax(ylabel="Score")
        ax_qual.plot(df_qual["timestamp"], df_qual["quality_score"], color=ACCENT, linewidth=2, zorder=2)
        
        df_pass = df_qual[df_qual["passed"] == 1]
        df_fail = df_qual[df_qual["passed"] == 0]
        if not df_pass.empty:
            ax_qual.scatter(df_pass["timestamp"], df_pass["quality_score"], color="#10b981", s=50, label="Pass", zorder=3)
        if not df_fail.empty:
            ax_qual.scatter(df_fail["timestamp"], df_fail["quality_score"], color="#ef4444", s=50, label="Fail", zorder=3)
            
        ax_qual.axhline(80, color=TEXT_LO, linestyle='--', linewidth=1, label="Min threshold", zorder=1)
        ax_qual.set_ylim(0, 105)
        ax_qual.legend(frameon=False, labelcolor='#cbd5e1', loc='lower right')
        st.pyplot(fig_qual, transparent=True)
        plt.close(fig_qual)
    else:
        st.info("No quality log entries yet.")

    st.subheader("Recent Pipeline Runs")
    df_log = _query("""
        SELECT run_id, started_at, finished_at, rows_fetched, rows_saved, status, error
        FROM pipeline_log ORDER BY id DESC LIMIT 20
    """)

    if df_log.empty:
        _no_data_msg()
    else:
        try:
            df_log["started_dt"]  = pd.to_datetime(df_log["started_at"],  errors="coerce", utc=True)
            df_log["finished_dt"] = pd.to_datetime(df_log["finished_at"], errors="coerce", utc=True)
            df_log["duration_s"]  = (
                (df_log["finished_dt"] - df_log["started_dt"])
                .dt.total_seconds().round(1)
            )
        except Exception:
            df_log["duration_s"] = None

        total_runs   = len(df_log)
        success_runs = (df_log["status"] == "SUCCESS").sum()
        fail_runs    = total_runs - success_runs
        success_rate = round(100 * success_runs / total_runs, 1) if total_runs > 0 else 0

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Runs",   total_runs)
        m2.metric("Successful",   success_runs)
        m3.metric("Failed",       fail_runs)
        m4.metric("Success Rate", f"{success_rate}%")

        st.markdown("&nbsp;", unsafe_allow_html=True)

        display_df = df_log[[
            "run_id", "started_at", "rows_fetched", "rows_saved",
            "status", "duration_s", "error"
        ]].copy()
        display_df["run_id"] = display_df["run_id"].str[:8] + "..."
        display_df.columns = ["Run ID", "Started At", "Fetched", "Saved",
                               "Status", "Duration (s)", "Error"]
        st.dataframe(display_df, width="stretch", hide_index=True)
