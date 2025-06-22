import streamlit as st
import pandas as pd
from services import database_pg

st.set_page_config(page_title="Reversal Signal Indodax", layout="wide")
st.title("📈 Downtrend & Potensi Reversal Detector")

# Pilih coin
all_coins = ["btc_idr", "eth_idr", "bnb_idr", "doge_idr"]  # sesuaikan list coin
selected_coin = st.selectbox("🪙 Pilih Coin", all_coins)

# Pilih jumlah histori candle yang dicek
limit = st.slider("Jumlah data candle terakhir", 5, 50, 10, 1)

# Ambil data dari database
price_logs = database_pg.get_recent_price_volume(selected_coin, limit=limit)

if not price_logs:
    st.warning("❌ Belum ada histori harga untuk coin ini.")
else:
    # Ubah ke DataFrame
    df = pd.DataFrame(price_logs, columns=["Harga", "Volume"])
    df.index = range(1, len(df) + 1)

    # Cek trend turun
    consecutive_down = 0
    for i in range(len(df) - 1):
        if df.loc[i+1, "Harga"] > df.loc[i+2, "Harga"]:
            consecutive_down += 1
        else:
            consecutive_down = 0

    # Cek apakah candle terakhir rebound
    rebound = False
    if df.loc[1, "Harga"] > df.loc[2, "Harga"] * 1.005:
        rebound = True

    # Tampilkan chart harga
    st.line_chart(df["Harga"])

    # Hasil analisis
    st.subheader("📊 Analisis Sinyal")
    st.write(f"📉 Consecutive Down: {consecutive_down}x")
    if consecutive_down >= 3 and rebound:
        st.success("🚀 Potensi Reversal Terdeteksi!")
    elif consecutive_down >= 3:
        st.warning("⚠️ Downtrend masih kuat, belum ada sinyal reversal.")
    else:
        st.info("ℹ️ Trend normal atau naik.")

    # Tampilkan tabel harga
    st.dataframe(df, use_container_width=True)
