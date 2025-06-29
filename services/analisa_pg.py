import streamlit as st
import pandas as pd
import numpy as np
import ta
import mplfinance as mpf
import matplotlib.pyplot as plt

from services import database_pg

# --- Pastikan pool siap ---
database_pg.init_connection_pool()

# --- Ambil semua ticker ---
def get_all_tickers():
    try:
        results = database_pg.get_all_tickers()
        return results
    except Exception as e:
        st.error(f"❌ Error get_all_tickers: {e}")
        return []

# --- Ambil 30 daily closes terakhir ---
def get_last_30_daily_closes(ticker):
    try:
        return database_pg.get_last_30_daily_closes(ticker)
    except Exception as e:
        st.error(f"❌ Error get_last_30_daily_closes: {e}")
        return []

# --- Ambil n closes terakhir ---
def get_last_n_closes(ticker, n):
    try:
        return database_pg.get_last_n_closes(ticker, n)
    except Exception as e:
        st.error(f"❌ Error get_last_n_closes: {e}")
        return []

# --- Ambil histori harga full untuk candlestick ---
def get_full_price_data(ticker):
    try:
        rows = database_pg.execute_query(
            """SELECT timestamp, last FROM ticker_history
               WHERE ticker = %s ORDER BY timestamp ASC""",
            (ticker,), fetch=True
        )
        df = pd.DataFrame(rows, columns=['timestamp', 'close'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        return df
    except Exception as e:
        st.error(f"❌ Error get_full_price_data: {e}")
        return pd.DataFrame()

# --- Hitung indikator MA, RSI, BB ---
def calculate_indicators(df):
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['RSI'] = ta.momentum.rsi(df['close'], window=14)
    bb = ta.volatility.BollingerBands(df['close'], window=20)
    df['UpperBand'] = bb.bollinger_hband()
    df['MiddleBand'] = bb.bollinger_mavg()
    df['LowerBand'] = bb.bollinger_lband()
    df.fillna(method='bfill', inplace=True)
    return df

# --- Hitung support-resistance sederhana ---
def get_support_resistance_levels(prices):
    data = pd.Series(prices)
    counts = data.value_counts().sort_values(ascending=False)
    support = counts.index.min()
    resistance = counts.index.max()
    return support, resistance

# --- Chart candlestick (pakai 1H OHLC simulasi) ---
def plot_candlestick_chart(df, ticker):
    mc = mpf.make_marketcolors(up='g', down='r', inherit=True)
    s = mpf.make_mpf_style(marketcolors=mc)

    df_ohlc = df.resample('1H').agg({
        'close': 'last'
    }).dropna()

    df_ohlc['open'] = df_ohlc['close'].shift(1)
    df_ohlc['high'] = df_ohlc[['open', 'close']].max(axis=1)
    df_ohlc['low']  = df_ohlc[['open', 'close']].min(axis=1)
    df_ohlc.dropna(inplace=True)

    fig, _ = mpf.plot(
        df_ohlc[['open','high','low','close']],
        type='candle', style=s, title=f'{ticker} Candlestick Chart',
        returnfig=True
    )
    st.pyplot(fig)

# --- Chart harga + MA ---
def plot_price_chart(df, ticker):
    fig, ax = plt.subplots(figsize=(10,5))
    df['close'].plot(ax=ax, label='Close')
    if 'MA5' in df.columns:
        df['MA5'].plot(ax=ax, label='MA5')
    if 'MA20' in df.columns:
        df['MA20'].plot(ax=ax, label='MA20')
    ax.set_title(f"{ticker} Harga + Moving Average")
    ax.legend()
    st.pyplot(fig)
