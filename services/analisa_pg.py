import psycopg2
import streamlit as st
import numpy as np
import pandas as pd
from services.database_pg import get_all_tickers
from services.database_pg import get_conn

def get_last_n_closes(ticker, n=30):
    """Ambil N harga penutupan harian terakhir"""
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
            LIMIT %s
        """, (ticker, n))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [r[0] for r in rows]
    except psycopg2.Error as e:
        st.error(f"❌ Error get_last_n_closes: {e}")
        return []

def calculate_moving_average(closes, window=5):
    """Hitung Moving Average"""
    if len(closes) < window:
        return []
    return pd.Series(closes).rolling(window).mean().tolist()

def calculate_rsi(closes, period=14):
    """Hitung RSI"""
    if len(closes) < period + 1:
        return None

    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)

    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    rs = avg_gain / avg_loss if avg_loss != 0 else 0
    rsi = 100 - (100 / (1 + rs))
    return round(rsi, 2)
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
    except Exception as e:
        print(f"Error get_all_tickers: {e}")
        return []

def calculate_bollinger_bands(closes, window=20):
    """Hitung Bollinger Bands"""
    if len(closes) < window:
        return None, None, None

    series = pd.Series(closes)
    sma = series.rolling(window).mean()
    std = series.rolling(window).std()

    upper_band = sma + (2 * std)
    lower_band = sma - (2 * std)

    return upper_band.tolist(), sma.tolist(), lower_band.tolist()

def get_support_resistance_levels(closes):
    """Cari harga support & resistance yang paling sering muncul"""
    counts = pd.Series(closes).value_counts().sort_values(ascending=False)
    if counts.empty:
        return None, None
    support = counts.index.min()   # harga terendah yang sering muncul
    resistance = counts.index.max()  # harga tertinggi yang sering muncul
    return support, resistance
