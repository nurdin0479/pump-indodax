import streamlit as st
import pandas as pd
from datetime import datetime
from services import database_pg, detector
from streamlit_autorefresh import st_autorefresh

# Inisialisasi DB
database_pg.init_db()

# Setting halaman
st.set_page_config(page_title="Pump Detector Pro Indodax", layout="wide")
st.title("🚨 Indodax Pump Detector Profesional")

# Preset Sensitivity
preset = st.sidebar.radio("🎛️ Preset Sensitivity", ["Custom", "Aggressive", "Moderate", "Safe"], index=0)

if preset == "Aggressive":
    interval = 3
    price_threshold = 1.0
    volume_threshold = 30.0
elif preset == "Moderate":
    interval = 3
    price_threshold = 1.5
    volume_threshold = 50.0
elif preset == "Safe":
    interval = 5
    price_threshold = 2.0
    volume_threshold = 80.0
else:
    interval = st.sidebar.selectbox("⏱️ Interval Refresh (detik)", [3, 5, 10], index=0)
    price_threshold = st.sidebar.slider("📈 Threshold Harga (%)", 0.5, 5.0, 1.5, 0.1)
    volume_threshold = st.sidebar.slider("📊 Threshold Volume (%)", 10.0, 500.0, 50.0, 5.0)

# Auto refresh realtime
st_autorefresh(interval=interval * 1000, key="data_refresh")

st.write("📊 Monitoring harga realtime Indodax")

# Fetch data Indodax
data = detector.fetch_indodax_data()
detected_pumps = []

for d in data:
    ticker = d['ticker']
    last = d['last']
    vol_idr = d['vol_idr']

    # Simpan histori harga & volume
    database_pg.save_ticker_history(ticker, last, vol_idr)

    # Deteksi pump
    is_pump, result = detector.is_valid_pump(ticker, price_threshold, volume_threshold)
    if is_pump:
        detector.send_telegram_message(
            f"🚨 PUMP DETECTED {result['ticker'].upper()}\n"
            f"Harga: {result['harga_sebelum']} ➡️ {result['harga_sekarang']} (+{result['kenaikan_harga']:.2f}%)\n"
            f"Volume: +{result['kenaikan_volume']:.2f}%\n"
            f"MA Harga: {result['ma_harga']}\n"
            f"MA Volume: {result['ma_volume']}\n"
            f"Consecutive Up: {result['consecutive_up']}x\n"
            f"Jam: {result['timestamp']}"
        )
        detected_pumps.append(result)

# Tampilkan tabel pump kalau ada
if detected_pumps:
    st.subheader("📈 Pump Terdeteksi Saat Ini")
    st.dataframe(pd.DataFrame(detected_pumps))

# Waktu update terakhir
st.write(f"🕒 Update terakhir: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} WIB")
