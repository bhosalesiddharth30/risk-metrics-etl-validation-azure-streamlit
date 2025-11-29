import streamlit as st
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_FILE = PROJECT_ROOT / "data" / "processed" / "risk_snapshot_clean.csv"
REPORT_FILE = PROJECT_ROOT / "data" / "reports" / "validation_report.csv"


@st.cache_data
def load_data():
    if PROCESSED_FILE.exists():
        df = pd.read_csv(PROCESSED_FILE, parse_dates=["date"])
    else:
        df = pd.DataFrame()
    if REPORT_FILE.exists():
        report = pd.read_csv(REPORT_FILE)
    else:
        report = pd.DataFrame()
    return df, report


st.title("Risk Metrics ETL & Validation Dashboard")
st.markdown(
    "Demo showing daily risk metrics (P50/P95, spreads, delta, vol) "
    "and validation flags."
)

df, report = load_data()

if df.empty:
    st.warning("No processed data found. Run the validation script first.")
else:
    portfolios = sorted(df["portfolio"].unique())
    selected_port = st.selectbox("Portfolio", portfolios)

    subset = df[df["portfolio"] == selected_port]

    st.subheader(f"Metrics for {selected_port}")
    st.dataframe(subset)

    pivot = subset.pivot_table(
        index="date", columns="metric", values="value"
    )
    st.line_chart(pivot)

if not report.empty:
    st.subheader("Validation Summary")
    st.dataframe(report)

    flag = report["overall_flag"].iloc[0]
    st.markdown(f"### Overall Data Quality Flag: **{flag}**")
else:
    st.info("No validation report found yet.")
