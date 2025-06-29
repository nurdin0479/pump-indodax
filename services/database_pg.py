import psycopg2
from psycopg2 import pool
from urllib.parse import urlparse
from contextlib import contextmanager
import streamlit as st

# --- Connection Pool Config ---
DB_POOL = None
MAX_CONN = 5  # Aiven free / PostgreSQL Free tier limitation
CONN_TIMEOUT = 5

# --- Initialize Connection Pool ---
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
        print("✅ Database pool initialized")
    except Exception as e:
        st.error(f"❌ Failed to init DB pool: {e}")
        DB_POOL = None

# --- Get Pool Connection ---
def get_conn():
    if not DB_POOL:
        init_connection_pool()
    return DB_POOL.getconn()

# --- Release Connection ---
def release_conn(conn):
    if conn:
        try:
            DB_POOL.putconn(conn)
        except:
            conn.close()

# --- Context Manager for Safe Queries ---
@contextmanager
def get_conn_cursor():
    conn = get_conn()
    cursor = conn.cursor()
    try:
        yield conn, cursor
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        cursor.close()
        release_conn(conn)

# --- Execute General Query ---
def execute_query(query, params=None, fetch=False, fetchone=False, return_affected_rows=False):
    with get_conn_cursor() as (conn, cursor):
        cursor.execute(query, params)
        if fetchone:
            return cursor.fetchone()
        if fetch:
            return cursor.fetchall()
        if return_affected_rows:
            return cursor.rowcount
        return None

# --- Initialize DB Schema ---
def init_db_schema():
    queries = [
        """CREATE TABLE IF NOT EXISTS ticker_history (
            id SERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            last NUMERIC(18,8) NOT NULL,
            vol_idr NUMERIC(18,2) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT unique_ticker_timestamp UNIQUE (ticker, timestamp)
        )""",
        """CREATE INDEX IF NOT EXISTS idx_ticker_history_ticker ON ticker_history(ticker)""",
        """CREATE INDEX IF NOT EXISTS idx_ticker_history_timestamp ON ticker_history(timestamp)""",
        """CREATE TABLE IF NOT EXISTS pump_history (
            id SERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            harga_sebelum NUMERIC(18,8) NOT NULL,
            harga_sekarang NUMERIC(18,8) NOT NULL,
            kenaikan_harga NUMERIC(18,2) NOT NULL,
            kenaikan_volume NUMERIC(18,2) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        """CREATE INDEX IF NOT EXISTS idx_pump_history_ticker ON pump_history(ticker)""",
        """CREATE INDEX IF NOT EXISTS idx_pump_history_timestamp ON pump_history(timestamp)""",
        """CREATE TABLE IF NOT EXISTS price_event_log (
            id SERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            harga_sebelum NUMERIC(18,8),
            harga_sekarang NUMERIC(18,8),
            kenaikan_harga NUMERIC(18,2),
            kenaikan_volume NUMERIC(18,2),
            ma_harga NUMERIC(18,8),
            ma_volume NUMERIC(18,8),
            consecutive_up INT,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"""
    ]
    for q in queries:
        execute_query(q)
    print("✅ DB schema ensured")

# --- Save Ticker History ---
def save_ticker_history(ticker, last, vol_idr):
    execute_query(
        """INSERT INTO ticker_history (ticker, last, vol_idr)
           VALUES (%s, %s, %s)
           ON CONFLICT (ticker, timestamp) DO NOTHING""",
        (ticker, last, vol_idr)
    )

# --- Get All Tickers ---
def get_all_tickers():
    results = execute_query(
        "SELECT DISTINCT ticker FROM ticker_history ORDER BY ticker",
        fetch=True
    )
    return [r[0] for r in results] if results else []

# --- Get Recent Price/Volume ---
def get_recent_price_volume(ticker, limit=4):
    results = execute_query(
        """SELECT last, vol_idr FROM ticker_history
           WHERE ticker=%s ORDER BY timestamp DESC LIMIT %s""",
        (ticker, limit),
        fetch=True
    )
    return results or []

# --- Get Pump History ---
def get_pump_history(limit=50):
    results = execute_query(
        """SELECT ticker, harga_sebelum::numeric(18,8), harga_sekarang::numeric(18,8),
                  kenaikan_harga::numeric(18,2), kenaikan_volume::numeric(18,2),
                  timestamp::varchar(19)
           FROM pump_history
           ORDER BY timestamp DESC LIMIT %s""",
        (limit,), fetch=True
    )
    return results or []

# --- Get Price History Since ---
def get_price_history_since(ticker, since_date):
    results = execute_query(
        """SELECT last FROM ticker_history
           WHERE ticker=%s AND timestamp >= %s
           ORDER BY timestamp DESC""",
        (ticker, since_date),
        fetch=True
    )
    return results or []

# --- Get Last N Closes ---
def get_last_n_closes(ticker, n):
    results = execute_query(
        """SELECT last FROM ticker_history
           WHERE ticker=%s ORDER BY timestamp DESC LIMIT %s""",
        (ticker, n),
        fetch=True
    )
    return [r[0] for r in results] if results else []

# --- Get Last 30 Daily Closes ---
def get_last_30_daily_closes(ticker):
    results = execute_query(
        """SELECT last FROM (
               SELECT DISTINCT ON (DATE(timestamp)) DATE(timestamp) as tgl, last
               FROM ticker_history
               WHERE ticker=%s
               ORDER BY DATE(timestamp) DESC, timestamp DESC
           ) AS daily_prices
           ORDER BY tgl DESC LIMIT 30""",
        (ticker,), fetch=True
    )
    return [r[0] for r in results] if results else []

# --- Save Pump Log ---
def save_pump_log(data):
    execute_query(
        """INSERT INTO pump_history (ticker, harga_sebelum, harga_sekarang, 
            kenaikan_harga, kenaikan_volume)
           VALUES (%s, %s, %s, %s, %s)""",
        (data['ticker'], data['harga_sebelum'], data['harga_sekarang'],
         data['kenaikan_harga'], data['kenaikan_volume'])
    )

# --- Save Price Event Log ---
def save_price_event_log(data):
    execute_query(
        """INSERT INTO price_event_log
           (ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume,
            ma_harga, ma_volume, consecutive_up)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (data['ticker'], data['harga_sebelum'], data['harga_sekarang'],
         data['kenaikan_harga'], data['kenaikan_volume'],
         data['ma_harga'], data['ma_volume'], data['consecutive_up'])
    )
def check_db_health():
    """Cek apakah database bisa diakses"""
    try:
        result = execute_query("SELECT 1", fetchone=True)
        return result[0] == 1 if result else False
    except Exception as e:
        print(f"❌ DB Health Check Failed: {e}")
        return False

# --- Get Pool Status ---
def get_pool_status():
    if DB_POOL:
        return {
            'min_connections': DB_POOL.minconn,
            'max_connections': DB_POOL.maxconn,
            'connections_in_use': len(DB_POOL._used),
            'connections_available': len(DB_POOL._rused)
        }
    return None
