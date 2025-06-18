import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

# --- Konfigurasi Halaman ---
st.set_page_config(page_title="Forecast Saham NVIDIA 📈", layout="wide")
st.title("Dashboard Forecast Saham NVIDIA 📊")

# --- Path & Validasi File Forecast ---
forecast_path = "./src/ml/result/nvidia_high_forecast_5day.csv"
if not os.path.exists(forecast_path):
    st.error("❌ File forecast tidak ditemukan di 'src/ml/result/'. Pastikan file ada.")
    st.stop()

# --- Fungsi untuk Koneksi & Mengambil Data dari Database ---

# [PERUBAHAN] Koneksi database tanpa menggunakan st.secrets
# Menggunakan cache_resource untuk memastikan koneksi hanya dibuat sekali
@st.cache_resource
def init_connection():
    """
    Menginisialisasi koneksi ke database PostgreSQL.
    PERINGATAN: Menyimpan kredensial langsung di dalam kode seperti ini tidak aman
    dan tidak direkomendasikan untuk production. Gunakan hanya untuk development lokal.
    """
    try:
        # --- GANTI DENGAN KREDENSIAL DATABASE ANDA YANG SEBENARNYA ---
        db_dialect = "postgresql"
        db_username = os.getenv("DB_USERNAME")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST_LOCAL")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        # -------------------------------------------------------------

        engine_url = (
            f"{db_dialect}://"
            f"{db_username}:{db_password}@"
            f"{db_host}:{db_port}/{db_name}"
        )
        engine = create_engine(engine_url)
        return engine
    except Exception as e:
        st.error(f"❌ Gagal terhubung ke database. Periksa kembali detail koneksi Anda. Error: {e}")
        st.stop()


# Menggunakan cache_data untuk caching hasil query
@st.cache_data(ttl="1h") # Cache data selama 1 jam
def fetch_db_data(_engine):
    """
    Mengambil data historis dari tabel saham di PostgreSQL.
    """
    try:
        # GANTI 'nvidia_stock_history' dengan nama tabel Anda yang sebenarnya
        query = 'SELECT date_time, "open", "high", "low", "close", "volume" FROM stock_ohlcv ORDER BY date_time ASC;'
        df = pd.read_sql(query, _engine)
        
        # Mengganti nama kolom 'date_time' menjadi 'timestamp' agar sesuai dengan sisa script
        df = df.rename(columns={"date_time": "timestamp"})
        
        # Pastikan kolom timestamp berformat datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        
        # Pastikan kolom lain berformat numerik
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col])
            
        return df
    except Exception as e:
        st.error(f"❌ Gagal mengambil data dari tabel. Pastikan nama tabel dan kolom sudah benar. Error: {e}")
        st.stop()


# --- Memuat Data ---

# Memuat data Forecast
forecast_df = pd.read_csv(forecast_path, index_col=0, parse_dates=True)
forecast_df.index.name = "timestamp"
forecast_df = forecast_df.reset_index()

forecast_1d = forecast_df.iloc[:5]
forecast_5d = forecast_df.iloc[5:]

# Memuat data History dari PostgreSQL
engine = init_connection()
history_df = fetch_db_data(engine)

if history_df.empty:
    st.warning("⚠️ Data historis tidak ditemukan di database atau tabel kosong.")
    st.stop()


# --- Filter Rentang Tanggal ---
min_date = min(forecast_df["timestamp"].min(), history_df["timestamp"].min()).date()
max_date = max(forecast_df["timestamp"].max(), history_df["timestamp"].max()).date()

start_date, end_date = st.date_input(
    "📅 Pilih rentang tanggal",
    value=[min_date, max_date],
    min_value=min_date,
    max_value=max_date
)

start_datetime = pd.to_datetime(start_date)
end_datetime = pd.to_datetime(end_date)

history_filtered = history_df[
    (history_df["timestamp"] >= start_datetime) &
    (history_df["timestamp"] <= end_datetime)
]
forecast_1d_filtered = forecast_1d[
    (forecast_1d["timestamp"] >= start_datetime) &
    (forecast_1d["timestamp"] <= end_datetime)
]
forecast_5d_filtered = forecast_5d[
    (forecast_5d["timestamp"] >= start_datetime) &
    (forecast_5d["timestamp"] <= end_datetime)
]


# --- Tampilan Visual ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("📊 Forecast 1 Hari ke Depan")
    fig1d = go.Figure()
    fig1d.add_trace(go.Scatter(
        x=forecast_1d_filtered["timestamp"],
        y=forecast_1d_filtered["forecast"],
        mode="lines+markers",
        name="Forecast"
    ))
    fig1d.update_layout(xaxis_title="🕒 Waktu", yaxis_title="💰 Harga (USD)")
    st.plotly_chart(fig1d, use_container_width=True)

with col2:
    st.subheader("📊 Forecast 5 Hari ke Depan")
    fig5d = go.Figure()
    fig5d.add_trace(go.Scatter(
        x=forecast_5d_filtered["timestamp"],
        y=forecast_5d_filtered["forecast"],
        mode="lines+markers",
        name="Forecast"
    ))
    fig5d.update_layout(xaxis_title="🕒 Waktu", yaxis_title="💰 Harga (USD)")
    st.plotly_chart(fig5d, use_container_width=True)

# Grafik History Saham
st.subheader("📈 Grafik Historis Harga Saham (Close)")
fig_history = go.Figure()
fig_history.add_trace(go.Scatter(
    x=history_filtered["timestamp"],
    y=history_filtered["close"],
    mode="lines",
    name="Harga Penutupan"
))
fig_history.update_layout(xaxis_title="🕒 Waktu", yaxis_title="💰 Harga (USD)")
st.plotly_chart(fig_history, use_container_width=True)


# --- Preview Data ---
st.subheader("📋 Preview Data Forecast")
preview_forecast = forecast_df[["timestamp", "forecast"]].copy()
preview_forecast["timestamp"] = preview_forecast["timestamp"].dt.strftime('%Y-%m-%d %H:%M:%S')
st.dataframe(preview_forecast.head(10))

st.subheader("📋 Preview Data Historis dari Database")
preview_history = history_df[["timestamp", "open", "high", "low", "close", "volume"]].copy()
preview_history["timestamp"] = preview_history["timestamp"].dt.strftime('%Y-%m-%d %H:%M:%S')
st.dataframe(preview_history.head(10))