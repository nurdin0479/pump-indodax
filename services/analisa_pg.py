import streamlit as st
import pandas as pd
import numpy as np
import ta
import mplfinance as mpf
import matplotlib.pyplot as plt

from services import database_pg

# Inisialisasi koneksi pool sekali di awal
database_pg.init_connection_pool()

def get_all_tickers():
    try:
        conn = database_pg.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT ticker FROM ticker_history ORDER BY ticker")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [r[0] for r in rows]
    except Exception as e:
        st.error(f"❌ Error get_all_tickers: {e}")
        return []

def get_last_30_daily_closes(ticker):
    try:
        conn = database_pg.get_conn()
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
    except Exception as e:
        st.error(f"❌ Error get_last_30_daily_closes: {e}")
        return []

def get_last_n_closes(ticker, n):
    try:
        conn = database_pg.get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT last FROM ticker_history
            WHERE ticker = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (ticker, n))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [r[0] for r in rows][::-1]
    except Exception as e:
        st.error(f"❌ Error get_last_n_closes: {e}")
        return []

def get_full_price_data(ticker):
    try:
        conn = database_pg.get_conn()
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
    except Exception as e:
        st.error(f"❌ Error get_full_price_data: {e}")
        return pd.DataFrame()

def calculate_indicators(df):
    df['MA5'] = df['close'].rolling(window=5).mean()
    df['MA20'] = df['close'].rolling(window=20).mean()
    df['RSI'] = ta.momentum.rsi(df['close'], window=14)
    bb = ta.volatility.BollingerBands(df['close'], window=20)
    df['UpperBand'] = bb.bollinger_hband()
    df['MiddleBand'] = bb.bollinger_mavg()
    df['LowerBand'] = bb.bollinger_lband()
    df.fillna(method='bfill', inplace=True)
    return df

def get_support_resistance_levels(data):
    data = pd.Series(data)
    counts = data.value_counts().sort_values(ascending=False)
    support = counts.index.min()
    resistance = counts.index.max()
    return support, resistance

def plot_candlestick_chart(df, ticker):
    mc = mpf.make_marketcolors(up='g', down='r', inherit=True)
    s  = mpf.make_mpf_style(marketcolors=mc)

    df_ohlc = df.resample('1H').agg({
        'close': 'last'
    }).dropna()

    df_ohlc['open'] = df_ohlc['close'].shift(1)
    df_ohlc['high'] = df_ohlc[['open', 'close']].max(axis=1)
    df_ohlc['low']  = df_ohlc[['open', 'close']].min(axis=1)
    df_ohlc = df_ohlc.dropna()

    fig, axlist = mpf.plot(
        df_ohlc[['open','high','low','close']],
        type='candle',
        style=s,
        title=f'{ticker} Candlestick Chart',
        returnfig=True
    )
    st.pyplot(fig)

def plot_price_chart(df, ticker):
    fig, ax = plt.subplots(figsize=(10,5))
    df['close'].plot(ax=ax, label='Close Price')
    df['MA5'].plot(ax=ax, label='MA5')
    df['MA20'].plot(ax=ax, label='MA20')
    ax.set_title(f"{ticker} Price with Moving Averages")
    ax.legend()
    st.pyplot(fig)
