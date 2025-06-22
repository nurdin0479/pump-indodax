import streamlit as st
import pandas as pd
from services import database_pg

st.set_page_config(page_title="Valid Pump History", layout="wide")
st.title("📈 Log Pump Valid (Harga Naik 3x Berturut-turut)")

# Ambil data pump_history dari database
pump_logs = database_pg.get_pump_history(limit=100)

if not pump_logs:
    st.info("Belum ada log pump valid yang terekam.")
else:
    # Ubah jadi DataFrame
    df = pd.DataFrame(pump_logs, columns=[
        "Ticker", "Harga Sebelum", "Harga Sekarang",
        "Kenaikan Harga (%)", "Kenaikan Volume (%)", "Timestamp"
    ])

    # Tambahkan filter min kenaikan harga opsional
    min_kenaikan = st.sidebar.slider("Minimal Kenaikan Harga (%)", 0.5, 10.0, 2.0, 0.1)

    # Filter sesuai kenaikan harga
    df_filtered = df[df["Kenaikan Harga (%)"] >= min_kenaikan]

    st.write(f"📊 Menampilkan log pump dengan kenaikan harga minimal **{min_kenaikan}%**")
    st.dataframe(df_filtered, use_container_width=True)

    # Tampilkan jumlah total valid pump yang memenuhi syarat
    st.success(f"✅ Total valid pump: {len(df_filtered)}")
