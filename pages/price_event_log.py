import streamlit as st
import pandas as pd
from services import database_pg

st.set_page_config(page_title="Real-time Harga Coin", layout="wide")
st.title("📊 Log Harga Real-time per Coin (Debug View)")

# Pilih coin yang ingin dicek
all_coins = ["btc_idr", "eth_idr", "bnb_idr", "doge_idr"]  # tambah coin sesuai kebutuhan
selected_coin = st.selectbox("Pilih Coin", all_coins)

# Pilih jumlah histori yang ingin ditampilkan
limit = st.slider("Jumlah data terakhir", 5, 100, 10, 1)

# Fetch data dari database
price_logs = database_pg.get_recent_price_volume(selected_coin, limit=limit)

if not price_logs:
    st.warning("❌ Belum ada histori harga untuk coin ini.")
else:
    # Ubah ke DataFrame
    df = pd.DataFrame(price_logs, columns=["Harga", "Volume"])
    df.index = range(1, len(df) + 1)

    # Tampilkan tabel
    st.dataframe(df)

    # Tampilkan tren harga saja
    st.line_chart(df["Harga"])

    # Tampilkan tren volume saja
    st.line_chart(df["Volume"])
