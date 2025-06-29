import psycopg2
from psycopg2 import pool, OperationalError, InterfaceError, PoolError
from urllib.parse import urlparse
import streamlit as st
import time
from contextlib import contextmanager

# --- Connection Pool Configuration ---
DB_POOL = None
MAX_CONN = 10
CONN_TIMEOUT = 5
RETRY_DELAY = 1

# --- Initialize connection pool ---
def init_connection_pool():
    global DB_POOL
    if DB_POOL:
        return
    try:
        db_url = st.secrets["DATABASE_URL"]
        result = urlparse(db_url)
        DB_POOL = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=MAX_CONN,
            dbname=result.path[1:],
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port,
            sslmode="require",
            connect_timeout=CONN_TIMEOUT
        )
        print("✅ DB Pool initialized")
    except Exception as e:
        st.error(f"❌ Failed initialize connection pool: {e}")
        DB_POOL = None

def get_connection():
    global DB_POOL
    if DB_POOL is None:
        init_connection_pool()
    try:
        return DB_POOL.getconn()
    except PoolError as e:
        st.error("❌ Koneksi database penuh, coba lagi nanti.")
        raise

def release_connection(conn):
    if conn:
        try:
            if DB_POOL and not conn.closed:
                DB_POOL.putconn(conn)
        except:
            if not conn.closed:
                conn.close()

def close_all_connections():
    global DB_POOL
    if DB_POOL:
        DB_POOL.closeall()
        DB_POOL = None
        print("✅ Semua koneksi pool ditutup")

# --- Connection context manager ---
@contextmanager
def get_conn_cursor():
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        yield conn, cursor
        conn.commit()
    except (OperationalError, InterfaceError, PoolError) as e:
        if conn:
            conn.rollback()
        st.error(f"❌ DB Error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)

# --- Execute Query General ---
def execute_query(query, params=None, fetch=False, fetchone=False):
    with get_conn_cursor() as (conn, cursor):
        cursor.execute(query, params)
        if fetchone:
            return cursor.fetchone()
        if fetch:
            return cursor.fetchall()

# --- Custom Queries ---
def get_all_tickers():
    try:
        results = execute_query(
            "SELECT DISTINCT ticker FROM ticker_history ORDER BY ticker",
            fetch=True
        )
        return [r[0] for r in results] if results else []
    except Exception as e:
        st.error(f"❌ Error get_all_tickers: {e}")
        return []

def get_price_history_since(ticker, since_date):
    try:
        results = execute_query(
            "SELECT last FROM ticker_history WHERE ticker=%s AND timestamp >= %s ORDER BY timestamp DESC",
            (ticker, since_date),
            fetch=True
        )
        return results or []
    except Exception as e:
        st.error(f"❌ Error get_price_history_since: {e}")
        return []

def get_last_30_daily_closes(ticker):
    try:
        results = execute_query(
            """
            SELECT last FROM (
                SELECT DISTINCT ON (DATE(timestamp)) DATE(timestamp) as tgl, last
                FROM ticker_history
                WHERE ticker = %s
                ORDER BY DATE(timestamp) DESC, timestamp DESC
            ) AS daily_prices
            ORDER BY tgl DESC
            LIMIT 30
            """,
            (ticker,),
            fetch=True
        )
        return [r[0] for r in results] if results else []
    except Exception as e:
        st.error(f"❌ Error get_last_30_daily_closes: {e}")
        return []

def save_price_event_log(data):
    try:
        execute_query(
            """
            INSERT INTO price_event_log (ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume, ma_harga, ma_volume, consecutive_up, timestamp)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                data['ticker'], data['harga_sebelum'], data['harga_sekarang'],
                data['kenaikan_harga'], data['kenaikan_volume'],
                data['ma_harga'], data['ma_volume'], data['consecutive_up'],
                data['timestamp']
            )
        )
    except Exception as e:
        st.error(f"❌ Error save_price_event_log: {e}")

def save_pump_log(data):
    try:
        execute_query(
            """
            INSERT INTO pump_history (ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                data['ticker'], data['harga_sebelum'], data['harga_sekarang'],
                data['kenaikan_harga'], data['kenaikan_volume'], data['timestamp']
            )
        )
    except Exception as e:
        st.error(f"❌ Error save_pump_log: {e}")

# --- Pool Status ---
def get_pool_status():
    if DB_POOL:
        return {
            'minconn': DB_POOL.minconn,
            'maxconn': DB_POOL.maxconn,
            'in_use': len(DB_POOL._used),
            'available': len(DB_POOL._rused)
        }
    return None

# --- DB Health Check ---
def check_db_health():
    try:
        result = execute_query("SELECT 1", fetchone=True)
        return result[0] == 1
    except:
        return False

# --- Auto init if not yet ---
if 'DB_INITIALIZED' not in st.session_state:
    try:
        init_connection_pool()
        st.session_state.DB_INITIALIZED = True
    except Exception as e:
        st.error(f"❌ Database initialization failed: {e}")
        close_all_connections()
