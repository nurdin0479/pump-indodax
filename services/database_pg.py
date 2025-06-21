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
        sslmode=st.secrets["DB_SSLMODE"]
    )

def init_db():
    """Create table kalau belum ada"""
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS ticker_history (
                id SERIAL PRIMARY KEY,
                ticker TEXT,
                last REAL,
                vol_idr REAL,
                timestamp TEXT
            )
        """)

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
