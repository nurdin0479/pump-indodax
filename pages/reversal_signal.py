import streamlit as st
import pandas as pd
from services import database_pg
import numpy as np

st.set_page_config(page_title="📈 Reversal Signal Detector", layout="wide")
st.title("📈 Reversal Signal Indodax (Breakout MA 5-9-14)")

try:
    # --- Ambil semua coin dari database ---
    all_coins = database_pg.get_all_tickers()
    if not all_coins:
        st.warning("⚠️ Belum ada histori harga di database.")
        st.stop()

    # --- Parameter periode analisis ---
    periode_cek = st.sidebar.slider("Jumlah hari histori dicek", 5, 30, 7, 1)

    hasil_reversal = []

    for coin in all_coins:
        harga_data = database_pg.get_last_30_daily_closes(coin)
        if len(harga_data) < (periode_cek + 5):
            continue  # skip kalau histori kurang

        harga_series = pd.Series(harga_data[::-1])  # urut lama ke baru

        # Hitung MA
        ma5 = harga_series.rolling(5).mean()
        ma9 = harga_series.rolling(9).mean()
        ma14 = harga_series.rolling(14).mean()

        # Cek apakah selama periode n hari harga selalu di bawah semua MA
        is_downtrend = all(
            (harga_series[i] <= ma5[i]) and (harga_series[i] <= ma9[i]) and (harga_series[i] <= ma14[i])
            for i in range(-periode_cek-1, -1)
        )

        if not is_downtrend:
            continue

        # Cek apakah harga terakhir breakout ke atas semua MA
        harga_terakhir = harga_series.iloc[-1]
        if (harga_terakhir > ma5.iloc[-1]) and (harga_terakhir > ma9.iloc[-1]) and (harga_terakhir > ma14.iloc[-1]):
            hasil_reversal.append({
                "Ticker": coin,
                "Harga Terakhir": harga_terakhir,
                "MA5": round(ma5.iloc[-1], 2),
                "MA9": round(ma9.iloc[-1], 2),
                "MA14": round(ma14.iloc[-1], 2),
            })

    # --- Tampilkan hasil ---
    if hasil_reversal:
        st.success(f"✅ {len(hasil_reversal)} coin reversal terdeteksi.")
        st.dataframe(pd.DataFrame(hasil_reversal), use_container_width=True)
    else:
        st.info("ℹ️ Belum ada coin reversal hari ini.")

except Exception as e:
    st.error(f"❌ Error saat memproses data: {e}")
