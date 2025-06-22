import streamlit as st
from services import analisa_pg
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# === Set Halaman ===
st.set_page_config(page_title="Analisa Candle Pro", layout="wide")
st.title("📊 Analisa Candle Pro (Matplotlib + TradingView)")

# === Pilihan Coin ===
tickers = analisa_pg.get_all_tickers()
if not tickers:
    st.warning("❌ Data coin belum tersedia.")
    st.stop()

selected_ticker = st.selectbox("Pilih Coin", tickers)

# === Tombol Analisa ===
if st.button("🔍 Mulai Analisa"):
    st.subheader(f"📈 Chart Candle Matplotlib: {selected_ticker.upper()}")

    # Ambil data close harga terakhir
    closes = analisa_pg.get_last_n_closes(selected_ticker, 30)
    if len(closes) < 5:
        st.warning("Data candle tidak cukup untuk ditampilkan.")
    else:
        # Buat dataframe candle simulasi OHLC dari close
        df = pd.DataFrame({
            'close': closes,
            'open': closes[:-1] + np.random.uniform(-0.5, 0.5, len(closes)-1).tolist() + [closes[-1]],
        })
        df['high'] = df[['open', 'close']].max(axis=1) + np.random.uniform(0, 0.3, len(closes))
        df['low'] = df[['open', 'close']].min(axis=1) - np.random.uniform(0, 0.3, len(closes))
        df['date'] = pd.date_range(end=pd.Timestamp.today(), periods=len(closes))

        # Plot Chart Candle Matplotlib
        fig, ax = plt.subplots(figsize=(12, 5))
        for idx, row in df.iterrows():
            color = 'green' if row['close'] >= row['open'] else 'red'
            ax.plot([idx, idx], [row['low'], row['high']], color='black')
            ax.add_patch(plt.Rectangle(
                (idx - 0.3, min(row['open'], row['close'])),
                0.6,
                abs(row['open'] - row['close']),
                color=color
            ))

        ax.set_xticks(range(0, len(df), 5))
        ax.set_xticklabels(df['date'].dt.strftime('%m-%d').iloc[::5])
        ax.set_title(f'Candlestick Chart {selected_ticker.upper()} (30 Daily Closes)')
        ax.grid(True)
        st.pyplot(fig)

    st.subheader(f"📊 TradingView Chart: {selected_ticker.upper()}")

    # TradingView Embed
    pair = selected_ticker.upper()
    tradingview_url = f"https://s.tradingview.com/widgetembed/?frameElementId=tradingview_12c5a&symbol=INDODAX:{pair}&interval=D&theme=dark&style=1&timezone=Asia%2FJakarta"
    st.components.v1.iframe(tradingview_url, height=500)

else:
    st.info("Silakan pilih coin dan klik **Mulai Analisa**.")

