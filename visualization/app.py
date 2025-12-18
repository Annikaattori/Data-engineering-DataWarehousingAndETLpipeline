import os
from pathlib import Path
from datetime import datetime, date, time, timezone, timedelta

import pandas as pd
import streamlit as st
import altair as alt
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from src.data_processing.config import CONFIG
from src.data_processing.transformations import LOGGER

st.set_page_config(layout="wide")
st.title("Sääasemat – Temperature & Humidity (päiväkohtainen näkymä BigQuerystä)")

# Kiinteä UTC+2 (ei kesäaikaa)
LOCAL_TZ = timezone(timedelta(hours=2))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PAR_DIR = os.path.join(BASE_DIR, os.pardir)
FILEPATH = os.path.join(PAR_DIR, "keys", "bigquery", "api_key.json")
LOGGER.info(f"Using API key file (relative): {FILEPATH}")

@st.cache_resource
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=CONFIG.bigquery_project)

def table_fqn() -> str:
    return f"`{CONFIG.bigquery_project}.{CONFIG.bigquery_dataset}.{CONFIG.hourly_table}`"

def day_bounds_utc(d: date) -> tuple[datetime, datetime]:
    # Päivärajat paikallisessa UTC+2-ajassa -> UTC:ksi (BigQuery TIMESTAMP vertailua varten)
    start_local = datetime.combine(d, time.min, tzinfo=LOCAL_TZ)
    end_local = datetime.combine(d, time.max, tzinfo=LOCAL_TZ)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

@st.cache_data(ttl=60)
def fetch_day_data(day_start_utc: datetime, day_end_utc: datetime) -> pd.DataFrame:
    query = f"""
      SELECT
        station_id,
        station_name,
        timestamp,
        temperature,
        humidity
      FROM {table_fqn()}
      WHERE timestamp >= @start_ts
        AND timestamp <= @end_ts
      ORDER BY timestamp ASC
    """

    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("start_ts", "TIMESTAMP", day_start_utc),
            ScalarQueryParameter("end_ts", "TIMESTAMP", day_end_utc),
        ]
    )

    df = get_bq_client().query(query, job_config=job_config).to_dataframe()

    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["station_name"] = df["station_name"].astype(str)

    return df

# --- UI: päivävalinta (oletus: tänään) ---
selected_day = st.sidebar.date_input("Päivä (UTC+2)", value=date.today())
start_utc, end_utc = day_bounds_utc(selected_day)

# (Valinnainen) diagnostiikka credential-polusta
cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if cred_path:
    p = Path(cred_path).expanduser()
    # st.caption(f"GOOGLE_APPLICATION_CREDENTIALS: {p} | exists: {p.exists()}")

df = fetch_day_data(start_utc, end_utc)

if df.empty:
    st.warning(f"Valitulle päivälle ({selected_day}) ei löytynyt dataa.")
    st.stop()

st.caption(
    f"Päivä: {selected_day} (UTC+2) | UTC-rajat: {start_utc.isoformat()} – {end_utc.isoformat()} | "
    f"Rivejä: {len(df)} | Asemia: {df['station_name'].nunique()}"
)

base = alt.Chart(df).encode(
    x=alt.X(
        "timestamp:T",
        title="Aika (UTC)",
        axis=alt.Axis(format="%H:%M")  # 24h
    ),
    color=alt.Color("station_name:N", title="Sääasema"),
)

temp_chart = base.mark_line().encode(
    y=alt.Y("temperature:Q", title="Temperature (°C)"),
    tooltip=[
        "station_name:N",
        alt.Tooltip("timestamp:T", format="%Y-%m-%d %H:%M"),  # 24h tooltip
        alt.Tooltip("temperature:Q", format=".1f"),
    ],
).properties(height=350, title="Temperature")

hum_chart = base.mark_line().encode(
    y=alt.Y("humidity:Q", title="Humidity (%)"),
    tooltip=[
        "station_name:N",
        alt.Tooltip("timestamp:T", format="%Y-%m-%d %H:%M"),  # 24h tooltip
        alt.Tooltip("humidity:Q", format=".0f"),
    ],
).properties(height=350, title="Humidity")

st.altair_chart(temp_chart, use_container_width=True)
st.altair_chart(hum_chart, use_container_width=True)

st.subheader("Data (valittu päivä)")
st.dataframe(df, use_container_width=True)
