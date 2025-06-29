import streamlit as st
import pandas as pd
from services import database_pg
import matplotlib.pyplot as plt
import mplfinance as mpf

# Inisialisasi pool saat import pertama
database_pg.init_connection_pool()

def get_all_tickers():
    try:
        conn = database_pg.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT ticker FROM ticker_history ORDER BY ticker")
        rows = cur.fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        st.error(f"❌ Error get_all_tickers: {e}")
        return []
    finally:
        cur.close()
        database_pg.release_connection(conn)

def get_last_30_daily_closes(ticker):
    try:
        conn = database_pg.get_connection()
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
        return [r[0] for r in rows]
    except Exception as e:
        st.error(f"❌ Error get_last_30_daily_closes: {e}")
        return []
    finally:
        cur.close()
        database_pg.release_connection(conn)

def get_last_n_closes(ticker, n):
    try:
        conn = database_pg.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT last FROM ticker_history
            WHERE ticker = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (ticker, n))
        rows = cur.fetchall()
        return [r[0] for r in rows][::-1]  # dari yang lama ke terbaru
    except Exception as e:
        st.error(f"❌ Error get_last_n_closes: {e}")
        return []
    finally:
        cur.close()
        database_pg.release_connection(conn)

def get_full_price_data(ticker):
    try:
        conn = database_pg.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT timestamp, last FROM ticker_history
            WHERE ticker = %s
            ORDER BY timestamp ASC
        """, (ticker,))
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=['timestamp', 'close'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        return df
    except Exception as e:
        st.error(f"❌ Error get_full_price_data: {e}")
        return pd.DataFrame()
    finally:
        cur.close()
        database_pg.release_connection(conn)

import ta  # pastikan sudah di-import di atas jika belum

def calculate_indicators(df):
    """Hitung MA5, MA20, RSI, Bollinger Bands ke DataFrame harga"""
    try:
        df['MA5'] = df['close'].rolling(window=5).mean()
        df['MA20'] = df['close'].rolling(window=20).mean()

        df['RSI'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()

        bb = ta.volatility.BollingerBands(df['close'], window=20)
        df['UpperBand'] = bb.bollinger_hband()
        df['MiddleBand'] = bb.bollinger_mavg()
        df['LowerBand'] = bb.bollinger_lband()

        df.fillna(method='bfill', inplace=True)
        return df

    except Exception as e:
        st.error(f"❌ Error calculate_indicators: {e}")
        return df

def get_support_resistance_levels(data):
    """Cari level support & resistance sederhana dari data harga"""
    try:
        data = pd.Series(data)
        unique_prices = data.round(-2).value_counts().sort_values(ascending=False)
        support = unique_prices.index.min()
        resistance = unique_prices.index.max()
        return support, resistance
    except Exception as e:
        st.error(f"❌ Error get_support_resistance_levels: {e}")
        return None, None

def plot_price_chart(df, ticker):
    """Plot harga closing + MA5 & MA20"""
    try:
        fig, ax = plt.subplots(figsize=(10,5))
        df['close'].plot(ax=ax, label='Close Price', color='black')
        if 'MA5' in df.columns:
            df['MA5'].plot(ax=ax, label='MA5', color='blue')
        if 'MA20' in df.columns:
            df['MA20'].plot(ax=ax, label='MA20', color='orange')
        ax.set_title(f"{ticker} - Price + MA5 MA20")
        ax.legend()
        plt.tight_layout()
        st.pyplot(fig)
    except Exception as e:
        st.error(f"❌ Error plot_price_chart: {e}")

def plot_candlestick_chart(df, ticker):
    """Plot candlestick chart"""
    try:
        mc = mpf.make_marketcolors(up='green', down='red', inherit=True)
        s = mpf.make_mpf_style(marketcolors=mc)

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
            title=f'{ticker} - Candlestick Chart',
            returnfig=True
        )
        st.pyplot(fig)
    except Exception as e:
        st.error(f"❌ Error plot_candlestick_chart: {e}")        