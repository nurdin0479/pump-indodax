import streamlit as st
import pandas as pd
from datetime import datetime
from services import database_pg, detector

# Init DB
database_pg.init_db()

# Setting page
st.set_page_config(page_title="Pump Detector Pro Indodax", layout="wide")
st.title("🚨 Indodax Pump Detector Profesional")

# Preset Sensitivity
preset = st.sidebar.radio("🎛️ Preset Sensitivity", ["Custom", "Aggressive", "Moderate", "Safe"], index=0)

if preset == "Aggressive":
    interval = 2
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
    interval = st.sidebar.selectbox("Interval (detik)", [2, 3, 5, 10], index=0)
    price_threshold = st.sidebar.slider("Threshold Harga (%)", 0.5, 5.0, 1.5, 0.1)
    volume_threshold = st.sidebar.slider("Threshold Volume Spike (%)", 10.0, 500.0, 50.0, 5.0)

# Tombol manual refresh
if st.button("🔄 Manual Refresh Sekarang"):
    st.rerun()

st.write("📊 Monitoring harga realtime dari Indodax")

data = detector.fetch_indodax_data()
detected_pumps = []

for d in data:
    ticker = d['ticker']
    last = d['last']
    vol_idr = d['vol_idr']

    database_pg.save_ticker_history(ticker, last, vol_idr)

    is_pump, result = detector.is_valid_pump(ticker, price_threshold, volume_threshold)
    if is_pump:
        database_pg.save_pump_log(result)
        detector.send_telegram_message(
            f"🚨 PUMP DETECTED {result['ticker']}\n"
            f"Harga: {result['harga_sebelum']} ➡️ {result['harga_sekarang']} (+{result['kenaikan_harga']}%)\n"
            f"Volume: +{result['kenaikan_volume']}%\n"
            f"Jam: {result['timestamp']}"
        )
        detected_pumps.append(result)

if detected_pumps:
    st.subheader("📈 Pump Terdeteksi")
    st.dataframe(pd.DataFrame(detected_pumps))

st.write(f"🕒 Update terakhir: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} WIB")

# Auto refreshjjjj
st.cache_resource.clear()
st.rerun()