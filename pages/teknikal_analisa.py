import streamlit as st
from services import analisa_pg

st.title("📊 Analisa Teknikal Pro")

# Ambil daftar ticker dari database
tickers = analisa_pg.get_all_tickers()

if not tickers:
    st.warning("Data ticker belum tersedia.")
else:
    # Pilih koin dari selectbox
    selected_coin = st.selectbox("Pilih Coin", tickers)

    if st.button("🔍 Mulai Analisa"):
        closes = analisa_pg.get_last_n_closes(selected_coin, 30)
        if len(closes) < 5:
            st.error("Data kurang dari 5 candle, belum bisa analisa.")
        else:
            st.success(f"Data Harga Terakhir: {closes}")

            ma5 = pd.Series(closes).rolling(5).mean().tolist()
            st.write(f"MA 5: {ma5[-1]}")

            rsi = analisa_pg.calculate_indicators(
                pd.DataFrame({'close': closes})
            )['RSI'].iloc[-1]
            st.write(f"RSI: {rsi:.2f}")

            upper, sma, lower = (
                analisa_pg.calculate_indicators(
                    pd.DataFrame({'close': closes})
                )[["UpperBand", "MiddleBand", "LowerBand"]].iloc[-1]
            )
            st.write(f"Bollinger Bands - Upper: {upper:.2f}, SMA: {sma:.2f}, Lower: {lower:.2f}")

            support, resistance = analisa_pg.get_support_resistance_levels(closes)
            st.write(f"Support: {support}, Resistance: {resistance}")

            # Chart price + MA
            df = analisa_pg.get_full_price_data(selected_coin)
            df = analisa_pg.calculate_indicators(df)
            analisa_pg.plot_price_chart(df, selected_coin)
            analisa_pg.plot_candlestick_chart(df, selected_coin)
