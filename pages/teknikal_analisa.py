import streamlit as st
import pandas as pd
import numpy as np
from services import database_pg

st.set_page_config(page_title="Analisa Teknikal + Indikator", layout="wide")
st.title("📊 Analisa Teknikal 30 Candle + Indikator Populer")

# Ambil daftar coin dari database
all_coins = database_pg.get_all_tickers()

if not all_coins:
    st.warning("⚠️ Belum ada histori harga.")
    st.stop()

# Pilih coin
selected_coin = st.selectbox("🪙 Pilih Coin", all_coins)

# Tombol mulai analisa
if st.button("🚀 Mulai Analisa"):

    # Ambil 30 harga close terakhir
    closes = database_pg.get_last_30_daily_closes(selected_coin)

    if len(closes) < 10:
        st.error("❌ Data kurang dari 10 candle daily.")
        st.stop()

    closes = closes[::-1]  # urutkan dari lama ke terbaru
    st.success(f"✅ Ditemukan {len(closes)} data candle 1D.")

    df = pd.DataFrame(closes, columns=["Close"])

    # Moving Average (MA)
    df["MA5"] = df["Close"].rolling(window=5).mean()
    df["MA10"] = df["Close"].rolling(window=10).mean()
    df["MA20"] = df["Close"].rolling(window=20).mean()

    # RSI (Relative Strength Index)
    delta = df["Close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df["RSI"] = 100 - (100 / (1 + rs))

    # MACD (12-26 EMA)
    df["EMA12"] = df["Close"].ewm(span=12, adjust=False).mean()
    df["EMA26"] = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = df["EMA12"] - df["EMA26"]
    df["Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()

    # Bollinger Bands (20 MA + 2 SD)
    df["MA20"] = df["Close"].rolling(window=20).mean()
    df["STD20"] = df["Close"].rolling(window=20).std()
    df["UpperBB"] = df["MA20"] + (2 * df["STD20"])
    df["LowerBB"] = df["MA20"] - (2 * df["STD20"])

    # Tampilkan tabel hasil
    st.subheader("📊 Data + Indikator Teknikal")
    st.dataframe(df, use_container_width=True)

    # Chart harga + MA
    st.subheader("📈 Harga & Moving Average")
    st.line_chart(df[["Close", "MA5", "MA10", "MA20"]])

    # Chart RSI
    st.subheader("📈 RSI (Relative Strength Index)")
    st.line_chart(df["RSI"])

    # Chart MACD
    st.subheader("📈 MACD & Signal")
    st.line_chart(df[["MACD", "Signal"]])

    # Chart Bollinger Bands
    st.subheader("📈 Bollinger Bands")
    st.line_chart(df[["Close", "UpperBB", "LowerBB"]])
