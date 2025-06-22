import streamlit as st
from services import analisa_pg

st.title("📊 Analisa Teknikal")

# Ambil daftar ticker dari database
tickers = analisa_pg.get_all_tickers()

# Pilih coin dari selectbox
coin = st.selectbox("Pilih Coin", tickers)

if st.button("🔍 Mulai Analisa"):
    closes = analisa_pg.get_last_n_closes(coin, 30)

    if len(closes) < 5:
        st.error("❌ Data kurang dari 5 candle, belum bisa analisa.")
    else:
        st.write(f"📈 Data Harga Close Terakhir {len(closes)}: {closes}")

        # Moving Average
        ma5 = analisa_pg.calculate_moving_average(closes, window=5)
        st.write(f"📊 MA 5: {ma5[-1]:,.2f}")

        # RSI
        rsi = analisa_pg.calculate_rsi(closes)
        st.write(f"📉 RSI: {rsi:.2f}")

        # Bollinger Bands
        upper, sma, lower = analisa_pg.calculate_bollinger_bands(closes)
        st.write(f"📊 Bollinger Bands - Upper: {upper[-1]:,.2f}, SMA: {sma[-1]:,.2f}, Lower: {lower[-1]:,.2f}")

        # Support & Resistance
        support, resistance = analisa_pg.get_support_resistance_levels(closes)
        st.write(f"🛡️ Support: {support:,.2f}, 📈 Resistance: {resistance:,.2f}")
