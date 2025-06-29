import psycopg2
from psycopg2 import OperationalError, InterfaceError
from psycopg2.pool import SimpleConnectionPool, PoolError
from urllib.parse import urlparse
import streamlit as st
import time
from contextlib import contextmanager

# === Global Config ===
DB_POOL = None
MAX_CONN = 5
CONN_TIMEOUT = 5
RETRY_DELAY = 1


# === Connection Pool Management ===
def init_connection_pool():
    global DB_POOL
    if DB_POOL is not None:
        return

    try:
        db_url = st.secrets["DATABASE_URL"]
        result = urlparse(db_url)
        DB_POOL = SimpleConnectionPool(
            minconn=1,
            maxconn=MAX_CONN,
            dbname=result.path[1:],
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port,
            sslmode="require",
            connect_timeout=CONN_TIMEOUT,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        print("✅ Database pool initialized")
    except Exception as e:
        st.error(f"❌ Failed to initialize DB pool: {e}")
        DB_POOL = None


def close_all_connections():
    global DB_POOL
    if DB_POOL:
        try:
            DB_POOL.closeall()
            print("✅ All DB connections closed")
        except Exception as e:
            print(f"⚠️ Error closing connections: {e}")
        finally:
            DB_POOL = None


def check_db_health():
    try:
        result = execute_query("SELECT 1", fetchone=True)
        return result[0] == 1 if result else False
    except Exception:
        return False


# === Connection Utilities ===
def get_conn():
    global DB_POOL
    if DB_POOL is None:
        init_connection_pool()
    try:
        return DB_POOL.getconn()
    except PoolError:
        raise PoolError("❌ Connection pool exhausted. Increase MAX_CONN or check active queries.")
    except Exception as e:
        st.error(f"❌ DB connection error: {e}")
        raise


def release_conn(conn):
    global DB_POOL
    if conn:
        try:
            if DB_POOL and not conn.closed:
                DB_POOL.putconn(conn)
            elif not conn.closed:
                conn.close()
        except Exception:
            if not conn.closed:
                conn.close()


@contextmanager
def get_conn_cursor():
    conn = None
    cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        yield conn, cursor
        conn.commit()
    except (OperationalError, InterfaceError, PoolError) as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_conn(conn)


# === Query Executor ===
def execute_query(query, params=None, fetch=False, fetchone=False, return_rowcount=False):
    with get_conn_cursor() as (conn, cursor):
        cursor.execute(query, params)
        if fetchone:
            return cursor.fetchone()
        elif fetch:
            return cursor.fetchall()
        elif return_rowcount:
            return cursor.rowcount
        return None


# === Schema Init ===
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
            harga_sebelum NUMERIC(18,8),
            harga_sekarang NUMERIC(18,8),
            kenaikan_harga NUMERIC(18,2),
            kenaikan_volume NUMERIC(18,2),
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS price_event_log (
            id SERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            harga_sebelum NUMERIC(18,8),
            harga_sekarang NUMERIC(18,8),
            kenaikan_harga NUMERIC(18,2),
            kenaikan_volume NUMERIC(18,2),
            ma_harga NUMERIC(18,8),
            ma_volume NUMERIC(18,2),
            consecutive_up INTEGER,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    ]
    for q in queries:
        execute_query(q)


# === CRUD Method ===
def save_ticker_history(ticker, last, vol_idr):
    execute_query("""
        INSERT INTO ticker_history (ticker, last, vol_idr)
        VALUES (%s, %s, %s)
        ON CONFLICT (ticker, timestamp) DO NOTHING
    """, (ticker, last, vol_idr))


def get_recent_price_volume(ticker, limit=5):
    return execute_query("""
        SELECT last, vol_idr FROM ticker_history
        WHERE ticker = %s
        ORDER BY timestamp DESC
        LIMIT %s
    """, (ticker, limit), fetch=True)


def save_pump_log(data):
    execute_query("""
        INSERT INTO pump_history (ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        data['ticker'], data['harga_sebelum'], data['harga_sekarang'],
        data['kenaikan_harga'], data['kenaikan_volume']
    ))


def save_price_event_log(data):
    execute_query("""
        INSERT INTO price_event_log (ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume,
        ma_harga, ma_volume, consecutive_up, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        data['ticker'], data['harga_sebelum'], data['harga_sekarang'], data['kenaikan_harga'],
        data['kenaikan_volume'], data['ma_harga'], data['ma_volume'], data['consecutive_up'], data['timestamp']
    ))


def get_all_tickers():
    results = execute_query("SELECT DISTINCT ticker FROM ticker_history ORDER BY ticker", fetch=True)
    return [r[0] for r in results] if results else []


def get_price_history_since(ticker, since_date):
    results = execute_query("""
        SELECT last FROM ticker_history
        WHERE ticker = %s AND timestamp >= %s
        ORDER BY timestamp DESC
    """, (ticker, since_date), fetch=True)
    return results


def get_pump_history(limit=50):
    return execute_query("""
        SELECT ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume, timestamp
        FROM pump_history
        ORDER BY timestamp DESC
        LIMIT %s
    """, (limit,), fetch=True)


def get_last_30_daily_closes(ticker):
    results = execute_query("""
        SELECT last FROM (
            SELECT DISTINCT ON (DATE(timestamp)) DATE(timestamp), last
            FROM ticker_history
            WHERE ticker = %s
            ORDER BY DATE(timestamp) DESC, timestamp DESC
        ) AS daily_prices
        ORDER BY DATE DESC
        LIMIT 30
    """, (ticker,), fetch=True)
    return [r[0] for r in results] if results else []


def get_last_n_closes(ticker, n):
    results = execute_query("""
        SELECT last FROM ticker_history
        WHERE ticker = %s
        ORDER BY timestamp DESC
        LIMIT %s
    """, (ticker, n), fetch=True)
    return [r[0] for r in results][::-1] if results else []


def get_full_price_data(ticker):
    results = execute_query("""
        SELECT timestamp, last FROM ticker_history
        WHERE ticker = %s
        ORDER BY timestamp ASC
    """, (ticker,), fetch=True)
    return results or []


# === Auto Init Pool & Schema on Import ===
if 'DB_INITIALIZED' not in st.session_state:
    init_connection_pool()
    init_db_schema()
    st.session_state.DB_INITIALIZED = True
