import psycopg2
from psycopg2 import pool
from urllib.parse import urlparse
import streamlit as st
import time
from functools import wraps

# --- Global Connection Pool ---
DB_POOL = None
MAX_CONN = 5
CONN_TIMEOUT = 5
RETRY_DELAY = 1

# --- Retry Decorator ---
def with_db_retry(max_retries=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_count = 0
            while retry_count <= max_retries:
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    retry_count += 1
                    if retry_count > max_retries:
                        raise
                    time.sleep(RETRY_DELAY * retry_count)
        return wrapper
    return decorator

# --- Pool Management ---
def init_connection_pool():
    global DB_POOL
    if DB_POOL is not None:
        return
    try:
        db_url = st.secrets["DATABASE_URL"]
        result = urlparse(db_url)
        DB_POOL = psycopg2.pool.SimpleConnectionPool(
            1, MAX_CONN,
            dbname=result.path[1:],
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port,
            sslmode="require",
            connect_timeout=CONN_TIMEOUT
        )
        print("✅ DB connection pool initialized.")
    except Exception as e:
        st.error(f"❌ DB connection pool error: {str(e)}")

def get_conn():
    global DB_POOL
    if DB_POOL is None:
        init_connection_pool()
    return DB_POOL.getconn()

def release_conn(conn):
    if conn and DB_POOL:
        DB_POOL.putconn(conn)

def close_all_connections():
    global DB_POOL
    if DB_POOL:
        DB_POOL.closeall()
        DB_POOL = None

# --- Query Executor ---
@with_db_retry()
def execute_query(query, params=None, fetch=False, fetchone=False, return_affected_rows=False):
    conn, cursor, result = None, None, None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(query, params)
        if fetchone:
            result = cursor.fetchone()
        elif fetch:
            result = cursor.fetchall()
        elif return_affected_rows:
            result = cursor.rowcount
        conn.commit()
        return result
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        st.error(f"❌ DB Error: {e}")
        raise
    finally:
        if cursor: cursor.close()
        if conn: release_conn(conn)

# --- Database Schema Initialization ---
def init_db_schema():
    queries = [
        """
        CREATE TABLE IF NOT EXISTS ticker_history (
            id SERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            last NUMERIC(18,8) NOT NULL,
            vol_idr NUMERIC(18,2) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT unique_ticker_timestamp UNIQUE (ticker, timestamp)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS pump_history (
            id SERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            harga_sebelum NUMERIC(18,8) NOT NULL,
            harga_sekarang NUMERIC(18,8) NOT NULL,
            kenaikan_harga NUMERIC(18,2) NOT NULL,
            kenaikan_volume NUMERIC(18,2) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS price_event_log (
            id SERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            harga_sebelum NUMERIC(18,8) NOT NULL,
            harga_sekarang NUMERIC(18,8) NOT NULL,
            kenaikan_harga NUMERIC(18,2) NOT NULL,
            kenaikan_volume NUMERIC(18,2) NOT NULL,
            ma_harga NUMERIC(18,8) NOT NULL,
            ma_volume NUMERIC(18,2) NOT NULL,
            consecutive_up INTEGER NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_ticker_history_ticker ON ticker_history(ticker)",
        "CREATE INDEX IF NOT EXISTS idx_pump_history_ticker ON pump_history(ticker)"
    ]
    for q in queries:
        execute_query(q)
    print("✅ Database schema initialized.")

# --- CRUD Operations ---
def save_ticker_history(ticker, last, vol_idr):
    execute_query("""
        INSERT INTO ticker_history (ticker, last, vol_idr)
        VALUES (%s, %s, %s)
        ON CONFLICT (ticker, timestamp) DO NOTHING
    """, (ticker, last, vol_idr))

def get_recent_price_volume(ticker, limit=5):
    return execute_query("""
        SELECT last, vol_idr FROM ticker_history
        WHERE ticker = %s ORDER BY timestamp DESC LIMIT %s
    """, (ticker, limit), fetch=True) or []

def save_pump_log(data):
    execute_query("""
        INSERT INTO pump_history 
        (ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        data['ticker'],
        data['harga_sebelum'],
        data['harga_sekarang'],
        data['kenaikan_harga'],
        data['kenaikan_volume']
    ))

def save_price_event_log(data):
    execute_query("""
        INSERT INTO price_event_log 
        (ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume, ma_harga, ma_volume, consecutive_up, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        data['ticker'],
        data['harga_sebelum'],
        data['harga_sekarang'],
        data['kenaikan_harga'],
        data['kenaikan_volume'],
        data['ma_harga'],
        data['ma_volume'],
        data['consecutive_up'],
        data['timestamp']
    ))

def get_pump_history(limit=50):
    return execute_query("""
        SELECT ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume, timestamp
        FROM pump_history
        ORDER BY timestamp DESC
        LIMIT %s
    """, (limit,), fetch=True) or []

def get_all_tickers():
    return [r[0] for r in execute_query("""
        SELECT DISTINCT ticker FROM ticker_history ORDER BY ticker
    """, fetch=True) or []]

def get_price_history_since(ticker, since_date):
    return execute_query("""
        SELECT last FROM ticker_history
        WHERE ticker = %s AND timestamp >= %s
        ORDER BY timestamp DESC
    """, (ticker, since_date), fetch=True) or []

def get_last_30_daily_closes(ticker):
    return [r[0] for r in execute_query("""
        SELECT last FROM (
            SELECT DISTINCT ON (DATE(timestamp)) DATE(timestamp) as tgl, last
            FROM ticker_history
            WHERE ticker = %s
            ORDER BY DATE(timestamp) DESC, timestamp DESC
        ) AS daily_prices
        ORDER BY tgl DESC LIMIT 30
    """, (ticker,), fetch=True) or []]

def get_last_n_closes(ticker, n):
    return [r[0] for r in execute_query("""
        SELECT last FROM ticker_history
        WHERE ticker = %s ORDER BY timestamp DESC LIMIT %s
    """, (ticker, n), fetch=True) or []][::-1]

def get_full_price_data(ticker):
    rows = execute_query("""
        SELECT timestamp, last FROM ticker_history
        WHERE ticker = %s ORDER BY timestamp ASC
    """, (ticker,), fetch=True)
    import pandas as pd
    df = pd.DataFrame(rows, columns=['timestamp', 'close'])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df.set_index('timestamp') if not df.empty else pd.DataFrame()

# --- Health Check ---
def check_db_health():
    try:
        return execute_query("SELECT 1", fetchone=True)[0] == 1
    except:
        return False

def get_pool_status():
    if DB_POOL:
        return {
            'min_connections': DB_POOL.minconn,
            'max_connections': DB_POOL.maxconn,
            'connections_in_use': len(DB_POOL._used),
            'connections_available': len(DB_POOL._rused)
        }
    return None

# --- Auto Init on Import ---
if 'DB_INITIALIZED' not in st.session_state:
    try:
        init_connection_pool()
        init_db_schema()
        st.session_state.DB_INITIALIZED = True
    except Exception as e:
        st.error(f"❌ DB Init error: {e}")
        close_all_connections()
