import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import json
import os

st.set_page_config(page_title="Forecast Saham NVIDIA 📈", layout="wide")
st.title("Dashboard Forecast Saham NVIDIA 📊")

forecast_path = r"C:\Users\amali\Downloads\nvidia_high_forecast_5day.csv"
history_path = r"C:\Users\amali\Documents\RDVProject\NVDIA_stock.csv"

if not os.path.exists(forecast_path) or not os.path.exists(history_path):
    st.error("❌ File forecast atau history tidak ditemukan. Pastikan ada di folder 'data/'")
    st.stop()

forecast_df = pd.read_csv(forecast_path, parse_dates=["timestamp"])

forecast_1d = forecast_df.iloc[:5]
forecast_5d = forecast_df.iloc[5:]

# Load history
if history_path.endswith(".json"):
    with open(history_path, "r") as f:
        raw_json = json.load(f)
    timeseries = raw_json.get("Time Series (60min)", {})
    history_df = pd.DataFrame(timeseries).T.rename(columns={
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. volume": "volume"
    })
    history_df = history_df.astype(float)
    history_df.index = pd.to_datetime(history_df.index)
    history_df = history_df.reset_index().rename(columns={"index": "timestamp"})
else:
    history_df = pd.read_csv(history_path, parse_dates=["datetime"])
    history_df = history_df.rename(columns={"datetime": "timestamp"})

# Rentang tanggal
min_date = min(forecast_df["timestamp"].min(), history_df["timestamp"].min()).date()
max_date = max(forecast_df["timestamp"].max(), history_df["timestamp"].max()).date()
start_date, end_date = st.date_input(
    "📅 Pilih rentang tanggal",
    value=[min_date, max_date],
    min_value=min_date,
    max_value=max_date
)

# Filter berdasarkan tanggal
forecast_1d_filtered = forecast_1d[
    (forecast_1d["timestamp"].dt.date >= start_date) &
    (forecast_1d["timestamp"].dt.date <= end_date)
]
forecast_5d_filtered = forecast_5d[
    (forecast_5d["timestamp"].dt.date >= start_date) &
    (forecast_5d["timestamp"].dt.date <= end_date)
]
history_filtered = history_df[
    (history_df["timestamp"].dt.date >= start_date) &
    (history_df["timestamp"].dt.date <= end_date)
]

col1, col2 = st.columns(2)

with col1:
    st.subheader("📊 Forecast 1 Hari")
    fig1d = go.Figure()
    fig1d.add_trace(go.Scatter(
        x=forecast_1d_filtered["timestamp"],
        y=forecast_1d_filtered["forecast"],
        mode="lines+markers",
        name="Forecast"
    ))
    fig1d.update_layout(xaxis_title="🕒 Timestamp", yaxis_title="💰 Harga")
    st.plotly_chart(fig1d, use_container_width=True)

with col2:
    st.subheader("📊 Forecast 5 Hari")
    fig5d = go.Figure()
    fig5d.add_trace(go.Scatter(
        x=forecast_5d_filtered["timestamp"],
        y=forecast_5d_filtered["forecast"],
        mode="lines+markers",
        name="Forecast"
    ))
    fig5d.update_layout(xaxis_title="🕒 Timestamp", yaxis_title="💰 Harga")
    st.plotly_chart(fig5d, use_container_width=True)

# 
st.subheader("📈 Grafik History Saham")
fig_history = go.Figure()
fig_history.add_trace(go.Scatter(
    x=history_filtered["timestamp"],
    y=history_filtered["close"],
    mode="lines+markers",
    name="Close Price"
))
fig_history.update_layout(xaxis_title="🕒 Timestamp", yaxis_title="💰 Harga")
st.plotly_chart(fig_history, use_container_width=True)

# 
st.subheader("📋 Preview Data Forecast")
preview_forecast = forecast_1d_filtered[["timestamp", "forecast"]]
st.dataframe(preview_forecast.head(20))