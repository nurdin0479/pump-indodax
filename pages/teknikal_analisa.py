import streamlit as st
from services import analisa_pg
import pandas as pd

st.set_page_config(page_title="📊 Analisa Teknikal Pro", layout="wide")
st.title("📊 Analisa Teknikal Pro")

try:
    # ✅ Ambil daftar ticker dari database
    tickers = analisa_pg.get_all_tickers()

    if not tickers:
        st.warning("⚠️ Data ticker belum tersedia.")
        st.stop()

    # ✅ Pilih koin dari selectbox
    selected_coin = st.selectbox("Pilih Coin", tickers)

    # ✅ Tambah slider limit candle
    limit = st.slider("Jumlah Candle Terakhir", 10, 100, 30, 5)

    if st.button("🔍 Mulai Analisa"):
        closes = analisa_pg.get_last_n_closes(selected_coin, limit)

        if len(closes) < 5:
            st.error("❌ Data kurang dari 5 candle, belum bisa analisa.")
        else:
            st.success(f"📊 Data Harga Terakhir ({len(closes)} data): {closes}")

            # Moving Average
            ma5_series = pd.Series(closes).rolling(5).mean()
            if ma5_series.isnull().all():
                st.warning("⚠️ Data MA5 belum tersedia.")
            else:
                st.write(f"📊 MA 5 Terakhir: {ma5_series.dropna().iloc[-1]:.2f}")

            # RSI, Bollinger Bands dll via dataframe
            df = pd.DataFrame({'close': closes})
            df = analisa_pg.calculate_indicators(df)

            if pd.notnull(df['RSI'].iloc[-1]):
                st.write(f"📈 RSI: {df['RSI'].iloc[-1]:.2f}")

            st.write(
                f"📊 Bollinger Bands - Upper: {df['UpperBand'].iloc[-1]:.2f}, "
                f"SMA: {df['MiddleBand'].iloc[-1]:.2f}, "
                f"Lower: {df['LowerBand'].iloc[-1]:.2f}"
            )

            # Support Resistance
            support, resistance = analisa_pg.get_support_resistance_levels(closes)
            st.write(f"🛡️ Support: {support:.2f}, 📌 Resistance: {resistance:.2f}")

            # Chart price + MA
            df_full = analisa_pg.get_full_price_data(selected_coin)
            if df_full.empty:
                st.warning("📭 Belum ada histori harga lengkap untuk chart.")
            else:
                df_full = analisa_pg.calculate_indicators(df_full)

                st.subheader("📊 Grafik Harga + Moving Average")
                analisa_pg.plot_price_chart(df_full, selected_coin)

                st.subheader("📈 Candlestick Chart")
                analisa_pg.plot_candlestick_chart(df_full, selected_coin)

except Exception as e:
    st.error(f"❌ Error saat proses analisa: {e}")
