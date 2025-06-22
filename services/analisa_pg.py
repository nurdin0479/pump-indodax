import numpy as np
from services.database_pg import get_conn
import pandas as pd


def get_all_tickers():
    """Ambil semua ticker unik dari database"""
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
        print(f"❌ Error get_all_tickers: {e}")
        return []


def get_last_n_closes(ticker, n=30):
    """Ambil n harga close terakhir untuk coin tertentu"""
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
        return [r[0] for r in rows][::-1]  # urutan lama ke baru
    except Exception as e:
        print(f"❌ Error get_last_n_closes: {e}")
        return []


def calculate_moving_average(data, window=5):
    """Hitung Moving Average"""
    if len(data) < window:
        return []
    return pd.Series(data).rolling(window=window).mean().tolist()


def calculate_rsi(prices, period=14):
    """Hitung RSI (Relative Strength Index)"""
    if len(prices) < period:
        return None

    deltas = np.diff(prices)
    seed = deltas[:period]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    rs = up / down if down != 0 else 0
    rsi = 100 - (100 / (1 + rs))
    return round(rsi, 2)


def calculate_bollinger_bands(prices, window=20):
    """Hitung Bollinger Bands"""
    if len(prices) < window:
        return [], [], []

    series = pd.Series(prices)
    sma = series.rolling(window=window).mean()
    std = series.rolling(window=window).std()

    upper_band = sma + (2 * std)
    lower_band = sma - (2 * std)

    return upper_band.tolist(), sma.tolist(), lower_band.tolist()


def get_support_resistance_levels(prices):
    """Deteksi level support & resistance dari frekuensi harga terbanyak"""
    if not prices:
        return None, None

    prices_rounded = [round(p, -int(np.log10(p)) + 1) if p != 0 else 0 for p in prices]
    price_counts = pd.Series(prices_rounded).value_counts()

    support = price_counts.idxmax()  # harga yang paling sering muncul
    resistance_candidates = price_counts[price_counts.index > support]

    resistance = resistance_candidates.idxmax() if not resistance_candidates.empty else max(prices)
    return support, resistance
def get_last_n_closes(ticker, n=30):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT last FROM ticker_history
            WHERE ticker = %s
            ORDER BY id DESC
            LIMIT %s
        """, (ticker, n))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [r[0] for r in rows]
    except psycopg2.Error as e:
        st.error(f"❌ Error get_last_n_closes: {e}")
        return []
