import psycopg2
from psycopg2 import sql
import streamlit as st

def get_conn():
    return psycopg2.connect(
        dbname=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
        host=st.secrets["DB_HOST"],
        port=st.secrets["DB_PORT"],
        sslmode=st.secrets["DB_SSLMODE"],
        connect_timeout=10
    )

def init_db():
    """Create table kalau belum ada"""
    try:
        conn = get_conn()
        cur = conn.cursor()

        # Table ticker history
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ticker_history (
                id SERIAL PRIMARY KEY,
                ticker TEXT,
                last REAL,
                vol_idr REAL,
                timestamp TEXT
            )
        """)

        # Table pump history
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pump_history (
                id SERIAL PRIMARY KEY,
                ticker TEXT,
                harga_sebelum REAL,
                harga_sekarang REAL,
                kenaikan_harga REAL,
                kenaikan_volume REAL,
                timestamp TEXT
            )
        """)

        # Table price/volume event log
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_event_log (
                id SERIAL PRIMARY KEY,
                ticker TEXT,
                harga_sebelum REAL,
                harga_sekarang REAL,
                kenaikan_harga REAL,
                kenaikan_volume REAL,
                ma_harga REAL,
                ma_volume REAL,
                consecutive_up INTEGER,
                timestamp TEXT
            )
        """)

        conn.commit()
        cur.close()
        conn.close()

    except psycopg2.Error as e:
        st.error(f"❌ Error inisialisasi database: {e}")

def save_ticker_history(ticker, last, vol_idr):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ticker_history (ticker, last, vol_idr, timestamp)
            VALUES (%s, %s, %s, NOW())
        """, (ticker, last, vol_idr))
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        st.error(f"❌ Error save ticker history: {e}")

def get_recent_price_volume(ticker, limit=4):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT last, vol_idr FROM ticker_history
            WHERE ticker = %s
            ORDER BY id DESC
            LIMIT %s
        """, (ticker, limit))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except psycopg2.Error as e:
        st.error(f"❌ Error get_recent_price_volume: {e}")
        return []

def save_pump_log(data):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pump_history (ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            data['ticker'], data['harga_sebelum'], data['harga_sekarang'],
            data['kenaikan_harga'], data['kenaikan_volume'], data['timestamp']
        ))
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        st.error(f"❌ Error save_pump_log: {e}")

def get_pump_history(limit=50):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume, timestamp
            FROM pump_history
            ORDER BY id DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except psycopg2.Error as e:
        st.error(f"❌ Error get_pump_history: {e}")
        return []

def save_price_event_log(data):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO price_event_log 
            (ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume, ma_harga, ma_volume, consecutive_up, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data['ticker'], data['harga_sebelum'], data['harga_sekarang'],
            data['kenaikan_harga'], data['kenaikan_volume'], 
            data['ma_harga'], data['ma_volume'], data['consecutive_up'],
            data['timestamp']
        ))
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        st.error(f"❌ Error save_price_event_log: {e}")

def get_price_event_log(limit=50):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume, 
                   ma_harga, ma_volume, consecutive_up, timestamp
            FROM price_event_log
            ORDER BY id DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except psycopg2.Error as e:
        st.error(f"❌ Error get_price_event_log: {e}")
        return []
    
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

def get_price_history_since(ticker, since_date):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT last FROM ticker_history
            WHERE ticker = %s AND timestamp >= %s
            ORDER BY id DESC
        """, (ticker, since_date))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except psycopg2.Error as e:
        st.error(f"❌ Error get_price_history_since: {e}")
        return []

def get_last_30_daily_closes(ticker):
    """Ambil 30 harga terakhir per hari untuk analisis candle"""
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



