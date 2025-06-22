import streamlit as st
from services.database_pg import get_all_tickers
from services import analisa_pg

st.title("📊 Analisa Teknikal")
# Pilih koin dari database
coin = st.selectbox("Pilih Coin", get_all_tickers())

if st.button("🔍 Mulai Analisa"):
    closes = analisa_pg.get_last_n_closes(coin)
    if len(closes) < 5:
        st.error("Data kurang dari 5 candle, belum bisa analisa.")
    else:
        st.write(f"Data Harga Terakhir: {closes}")

        ma5 = analisa_pg.calculate_moving_average(closes, window=5)
        st.write(f"MA 5: {ma5[-1]}")

        rsi = analisa_pg.calculate_rsi(closes)
        st.write(f"RSI: {rsi}")

        upper, sma, lower = analisa_pg.calculate_bollinger_bands(closes)
        st.write(f"Bollinger Bands - Upper: {upper[-1]}, SMA: {sma[-1]}, Lower: {lower[-1]}")

        support, resistance = analisa_pg.get_support_resistance_levels(closes)
        st.write(f"Support: {support}, Resistance: {resistance}")
