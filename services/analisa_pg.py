# services/analisa_pg.py

import psycopg2
import streamlit as st
import pandas as pd
import numpy as np
import ta  # library technical analysis
import mplfinance as mpf
import matplotlib.pyplot as plt

def get_conn():
    return psycopg2.connect(
        dbname=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
        host=st.secrets["DB_HOST"],
        port=st.secrets["DB_PORT"],
        sslmode=st.secrets["DB_SSLMODE"]
    )

def get_all_tickers():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ticker FROM ticker_history
            ORDER BY ticker
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [r[0] for r in rows]
    except psycopg2.Error as e:
        st.error(f"❌ Error get_all_tickers: {e}")
        return []

def get_last_30_daily_closes(ticker):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT last FROM (
                SELECT DISTINCT ON (DATE(timestamp)) DATE(timestamp) as tgl, last
                FROM ticker_history
                WHERE ticker = %s
                ORDER BY DATE(timestamp) DESC, timestamp DESC
            ) AS daily_prices
            ORDER BY tgl DESC
            LIMIT 30
        """, (ticker,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [r[0] for r in rows]
    except psycopg2.Error as e:
        st.error(f"❌ Error get_last_30_daily_closes: {e}")
        return []

def get_full_price_data(ticker):
    """Ambil data lengkap harga & timestamp"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT timestamp, last FROM ticker_history
            WHERE ticker = %s
            ORDER BY timestamp ASC
        """, (ticker,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        df = pd.DataFrame(rows, columns=['timestamp', 'close'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        return df
    except psycopg2.Error as e:
        st.error(f"❌ Error get_full_price_data: {e}")
        return pd.DataFrame()

def calculate_indicators(df):
    df['MA5'] = df['close'].rolling(window=5).mean()
    df['MA20'] = df['close'].rolling(window=20).mean()
    df['RSI'] = ta.momentum.rsi(df['close'], window=14)
    df['UpperBand'], df['MiddleBand'], df['LowerBand'] = ta.volatility.BollingerBands(df['close']).bollinger_hband(), ta.volatility.BollingerBands(df['close']).bollinger_mavg(), ta.volatility.BollingerBands(df['close']).bollinger_lband()
    return df

def plot_candlestick_chart(df, ticker):
    mc = mpf.make_marketcolors(up='g', down='r', inherit=True)
    s  = mpf.make_mpf_style(marketcolors=mc)

    df_ohlc = df.resample('1H').agg({
        'close': 'last'
    }).dropna()

    df_ohlc['open'] = df_ohlc['close'].shift(1)
    df_ohlc['high'] = df_ohlc[['open', 'close']].max(axis=1)
    df_ohlc['low'] = df_ohlc[['open', 'close']].min(axis=1)

    df_ohlc = df_ohlc.dropna()

    fig, ax = mpf.plot(df_ohlc, type='candle', style=s, title=f'{ticker} Candlestick Chart', returnfig=True)
    st.pyplot(fig)

def plot_price_chart(df, ticker):
    fig, ax = plt.subplots(figsize=(10,5))
    df['close'].plot(ax=ax, label='Close Price')
    df['MA5'].plot(ax=ax, label='MA5')
    df['MA20'].plot(ax=ax, label='MA20')
    ax.set_title(f"{ticker} Price with Moving Averages")
    ax.legend()
    st.pyplot(fig)
