import streamlit as st
import pandas as pd
from collections import Counter
from services import database_pg

st.set_page_config(page_title="Analisa Teknikal Indodax", layout="wide")
st.title("📊 Analisa Teknikal Candle 30 Hari")

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

    st.success(f"✅ Ditemukan {len(closes)} data candle 1D terakhir.")

    # Hitung frekuensi kemunculan harga (dibulatkan ke 3 digit terdekat)
    rounded_prices = [round(p, -3) for p in closes]
    price_count = Counter(rounded_prices)

    # Tentukan harga support terkuat
    support_price, support_freq = price_count.most_common(1)[0]

    # Cari resistance atau TP berdasarkan harga-harga tertinggi
    sorted_prices = sorted(list(set(rounded_prices)))
    support_index = sorted_prices.index(support_price)

    tp1 = sorted_prices[support_index + 1] if support_index + 1 < len(sorted_prices) else None
    tp2 = sorted_prices[support_index + 2] if support_index + 2 < len(sorted_prices) else None
    tp3 = sorted_prices[support_index + 3] if support_index + 3 < len(sorted_prices) else None

    # Tampilkan hasil
    st.subheader("📈 Hasil Analisa Teknikal")
    st.write(f"**Support Terkuat:** Rp {support_price} (muncul {support_freq}x)")
    if tp1: st.write(f"**Take Profit 1:** Rp {tp1}")
    if tp2: st.write(f"**Take Profit 2:** Rp {tp2}")
    if tp3: st.write(f"**Take Profit 3:** Rp {tp3}")

    # Tampilkan tabel harga
    df = pd.DataFrame(closes, columns=["Harga Close"])
    st.dataframe(df)

    # Embed chart TradingView
    st.subheader("📊 Chart TradingView (Live)")

    tv_widget = f"""
    <iframe src="https://s.tradingview.com/widgetembed/?frameElementId=tradingview_{selected_coin}&symbol=INDODAX%3A{selected_coin.upper()}&interval=D&theme=dark&style=1&timezone=Asia%2FJakarta&hide_top_toolbar=true&save_image=false&studies=[]" 
        width="100%" height="480" frameborder="0" allowtransparency="true" scrolling="no"></iframe>
    """

    st.components.v1.html(tv_widget, height=480, scrolling=False)

