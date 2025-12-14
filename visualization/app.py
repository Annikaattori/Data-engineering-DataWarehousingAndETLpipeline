"""Streamlit app to visualise latest and long-term observations."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

from src.data_processing.config import CONFIG
from src.data_processing.fmi_client import FMIClient, observations_as_dataframe
from src.data_processing.transformations import build_long_term_tables

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def load_data():
    client = FMIClient(use_sample_data=True)
    observations = client.fetch_latest()
    return observations_as_dataframe(observations)


def main():  # pragma: no cover - UI code
    st.set_page_config(page_title="FMI Weather Pipeline", layout="wide")
    st.title("FMI Weather Pipeline Demo")

    latest_df = load_data()
    st.subheader("Most recent observations")
    st.dataframe(latest_df)

    st.subheader("Long-term tables (selected stations)")
    long_term_tables = build_long_term_tables(latest_df, CONFIG.station_whitelist)
    for station_id, station_df in long_term_tables.items():
        st.markdown(f"### Station {station_id}")
        st.line_chart(station_df.set_index("timestamp")["temperature"], height=200)
        st.line_chart(station_df.set_index("timestamp")["humidity"], height=200)

    st.markdown("---")
    st.caption("Data shown above uses bundled sample observations for offline demos.")


if __name__ == "__main__":
    main()
