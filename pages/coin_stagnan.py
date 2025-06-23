import streamlit as st
import pandas as pd
from services import database_pg
from datetime import timedelta, datetime

st.set_page_config(page_title="Coin Stagnan Detector", layout="wide")
st.title("📊 Coin Stagnan & Low Movement Detector")

# --- Caching data coin & price history
@st.cache_data(ttl=60)
def get_all_tickers():
    return database_pg.get_all_tickers()

@st.cache_data(ttl=60)
def get_price_history(ticker, since_date):
    return database_pg.get_price_history_since(ticker, since_date)

# --- Ambil semua coin dari database
all_coins = get_all_tickers()

if not all_coins:
    st.warning("⚠️ Belum ada histori harga di database.")
    st.stop()

# --- Filter periode hari & threshold analisis
day_range = st.sidebar.selectbox("Periode Analisis (hari)", [3, 7, 14, 30, 60], index=0)
range_threshold = st.sidebar.slider("📈 Max Range Harga (%)", 0.1, 5.0, 1.0, 0.1)
min_price = st.sidebar.number_input("Harga Minimal Coin (IDR)", value=0.0, step=500.0)

# --- Tanggal cutoff
cutoff_date = (datetime.now() - timedelta(days=day_range)).strftime('%Y-%m-%d %H:%M:%S')
st.write(f"📅 Analisis dari {cutoff_date} s.d. sekarang")

# --- Analisis koin stagnan
stagnan_coins = []

for coin in all_coins:
    logs = get_price_history(coin, cutoff_date)
    if len(logs) < 5:
        continue

    prices = [l[0] for l in logs]
    harga_max = max(prices)
    harga_min = min(prices)

    if harga_max == 0:
        continue

    price_range = ((harga_max - harga_min) / harga_min) * 100
    harga_terakhir = prices[0]

    if price_range <= range_threshold and harga_terakhir >= min_price:
        stagnan_coins.append({
            "Ticker": coin,
            "Harga Terkini": harga_terakhir,
            f"Range {day_range} Hari (%)": round(price_range, 3),
            "Data Point": len(prices)
        })

# --- Tampilkan hasil
if stagnan_coins:
    st.success(f"✅ {len(stagnan_coins)} coin stagnan ditemukan.")
    st.dataframe(pd.DataFrame(stagnan_coins), use_container_width=True)
else:
    st.info("ℹ️ Tidak ada coin stagnan di periode ini.")
