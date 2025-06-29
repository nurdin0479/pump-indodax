import streamlit as st
import pandas as pd
import numpy as np
import mplfinance as mpf
from datetime import datetime
from services import analisa_pg
import ta
import matplotlib.pyplot as plt

st.set_page_config(page_title="📊 Analisa Candle Pro", layout="wide")
st.title("📊 Analisa Candlestick & Indikator Pro")

try:
    # Ambil list coin
    coins = analisa_pg.get_all_tickers()
    if not coins:
        st.error("❌ Belum ada data ticker. Jalankan fetch harga dulu.")
        st.stop()

    # Pilih coin
    selected_coin = st.selectbox("Pilih Coin", coins)

    # Ambil harga close terakhir
    closes = analisa_pg.get_last_30_daily_closes(selected_coin)
    if len(closes) < 10:
        st.warning("⚠️ Data candle kurang dari 10 — minimal butuh 10 candle untuk analisa.")
        st.stop()

    # Simulasi open, high, low
    opens = [closes[0]]
    highs, lows = [], []
    for i in range(1, len(closes)):
        open_price = closes[i-1] + np.random.uniform(-0.5, 0.5)
        high_price = max(open_price, closes[i]) + np.random.uniform(0.1, 0.5)
        low_price = min(open_price, closes[i]) - np.random.uniform(0.1, 0.5)
        opens.append(open_price)
        highs.append(high_price)
        lows.append(low_price)
    highs.insert(0, max(opens[0], closes[0]) + np.random.uniform(0.1, 0.5))
    lows.insert(0, min(opens[0], closes[0]) - np.random.uniform(0.1, 0.5))

    # Buat DataFrame candle
    dates = pd.date_range(end=datetime.today(), periods=len(closes))
    df = pd.DataFrame({
        'Date': dates,
        'Open': opens,
        'High': highs,
        'Low': lows,
        'Close': closes
    }).set_index('Date')

    # Hitung indikator
    df['MA20'] = df['Close'].rolling(20).mean()
    df['Upper_BB'] = df['MA20'] + 2 * df['Close'].rolling(20).std()
    df['Lower_BB'] = df['MA20'] - 2 * df['Close'].rolling(20).std()
    df['RSI'] = ta.momentum.RSIIndicator(df['Close'], window=14).rsi()
    macd = ta.trend.MACD(df['Close'])
    df['MACD'] = macd.macd()
    df['MACD_signal'] = macd.macd_signal()

    # === Candlestick chart ===
    st.subheader("📈 Chart Candlestick")
    mc = mpf.make_marketcolors(up='green', down='red', inherit=True)
    s = mpf.make_mpf_style(marketcolors=mc)
    mpf_fig, _ = mpf.plot(df, type='candle', mav=(20,), volume=False, style=s, returnfig=True)
    st.pyplot(mpf_fig)

    # === Plot indikator tambahan ===
    st.subheader("📊 Indikator Teknis")
    fig, axs = plt.subplots(3, 1, figsize=(10, 8), sharex=True)

    # MA & Bollinger Bands
    axs[0].plot(df.index, df['Close'], label='Close', color='black')
    axs[0].plot(df.index, df['MA20'], label='MA20', color='blue')
    axs[0].fill_between(df.index, df['Upper_BB'], df['Lower_BB'], color='lightgray', alpha=0.4)
    axs[0].set_title("Harga + MA20 + Bollinger Bands")
    axs[0].legend()

    # RSI
    axs[1].plot(df.index, df['RSI'], label='RSI', color='purple')
    axs[1].axhline(70, color='red', linestyle='--')
    axs[1].axhline(30, color='green', linestyle='--')
    axs[1].set_title("RSI (14)")

    # MACD
    axs[2].plot(df.index, df['MACD'], label='MACD', color='blue')
    axs[2].plot(df.index, df['MACD_signal'], label='Signal', color='orange')
    axs[2].set_title("MACD")
    axs[2].legend()

    plt.tight_layout()
    st.pyplot(fig)

    st.success(f"✅ Data terakhir: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} WIB")

except Exception as e:
    st.error(f"❌ Error: {e}")
