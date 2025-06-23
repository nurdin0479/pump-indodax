import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from services import database_pg

st.set_page_config(page_title="📊 Market Summary & Season Detector", layout="wide")
st.title("📈 Market Summary & Season Detector 🔍")

# --- Config
start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
btc_ticker = "btc_idr"

# --- Function with caching to reduce database connections

@st.cache_data(ttl=60)
def get_price_history(ticker, since):
    return database_pg.get_price_history_since(ticker, since)

@st.cache_data(ttl=300)
def get_all_tickers():
    return database_pg.get_all_tickers()

# --- Ambil data harga BTC
btc_prices = get_price_history(btc_ticker, start_date)
if len(btc_prices) < 2:
    st.warning("⚠️ Data BTC belum cukup.")
    st.stop()

harga_awal_btc = btc_prices[-1][0]
harga_terakhir_btc = btc_prices[0][0]
btc_change = ((harga_terakhir_btc - harga_awal_btc) / harga_awal_btc) * 100

# --- Ambil BTC Dominance via CoinGecko
try:
    resp = requests.get("https://api.coingecko.com/api/v3/global")
    resp.raise_for_status()
    btc_dominance = resp.json()['data']['market_cap_percentage']['btc']
except Exception as e:
    btc_dominance = None
    st.error(f"❌ Gagal ambil BTC Dominance: {e}")

# --- Ambil semua altcoin selain BTC
all_tickers = get_all_tickers()
altcoins = [t for t in all_tickers if t != btc_ticker]

# --- Hitung altcoin yang outperform BTC
outperform_count = 0
alt_summary = []

for alt in altcoins:
    prices = get_price_history(alt, start_date)
    if len(prices) < 2:
        continue

    harga_awal = prices[-1][0]
    harga_terakhir = prices[0][0]
    change = ((harga_terakhir - harga_awal) / harga_awal) * 100

    if change > btc_change:
        outperform_count += 1

    alt_summary.append({
        "Ticker": alt,
        "Perubahan (%)": round(change, 2)
    })

# --- Hitung Altseason Strength
if len(alt_summary) > 0:
    altseason_strength = (outperform_count / len(alt_summary)) * 100
else:
    altseason_strength = 0

# --- Top Gainers & Losers
df_summary = pd.DataFrame(alt_summary)
df_gainers = df_summary.sort_values("Perubahan (%)", ascending=False).head(5)
df_losers = df_summary.sort_values("Perubahan (%)").head(5)

# --- Display Summary
st.subheader("📊 Market Summary")
col1, col2 = st.columns(2)
col1.metric("Kenaikan Harga BTC (%)", f"{btc_change:.2f}%")
col2.metric("BTC Dominance (%)", f"{btc_dominance:.2f}%" if btc_dominance else "N/A")

# --- Season Detector
st.subheader("🔥 Season Detector")
col3, col4 = st.columns(2)
col3.metric("Altseason Strength (%)", f"{altseason_strength:.2f}%")
if altseason_strength >= 75:
    col3.success("🔥 ALTCOIN SEASON!")
else:
    col3.info("Belum Altseason.")

col4.metric("BTC Season Strength (%)", f"{100 - altseason_strength:.2f}%")
if altseason_strength < 25:
    col4.success("💪 BTC Season!")
else:
    col4.info("Belum BTC Season.")

# --- Top Gainers & Losers
st.subheader("🚀 Top 5 Gainers (30 hari)")
st.dataframe(df_gainers, use_container_width=True)

st.subheader("🔻 Top 5 Losers (30 hari)")
st.dataframe(df_losers, use_container_width=True)

# --- Update terakhir
st.caption(f"🕒 Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} WIB")
