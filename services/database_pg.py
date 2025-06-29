import psycopg2
from psycopg2 import pool
from urllib.parse import urlparse
import streamlit as st
import time
from functools import wraps

# --- Connection Pool Configuration ---
DB_POOL = None
MAX_CONN = 5  # Aiven Free max 20
CONN_TIMEOUT = 5  # seconds
RETRY_DELAY = 1  # retry delay in seconds

# --- Decorators ---
def with_db_retry(max_retries=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_count = 0
            last_error = None
            while retry_count <= max_retries:
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    last_error = e
                    retry_count += 1
                    time.sleep(RETRY_DELAY * retry_count)
            raise last_error if last_error else Exception("Unknown database error")
        return wrapper
    return decorator

# --- Pool Initialization ---
def init_connection_pool():
    global DB_POOL
    if DB_POOL:
        return  # already initialized
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
            connect_timeout=CONN_TIMEOUT,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        print("✅ Connection pool initialized")
    except Exception as e:
        st.error(f"❌ Failed to init pool: {e}")
        DB_POOL = None

def ensure_initialized():
    """Ensure pool and schema are initialized"""
    if DB_POOL is None:
        init_connection_pool()
        init_db_schema()

# --- Connection Management ---
def get_connection():
    ensure_initialized()
    try:
        return DB_POOL.getconn()
    except psycopg2.pool.PoolError as e:
        print(f"⚠️ Pool exhausted: {e}")
        # fallback to direct connection
        db_url = st.secrets["DATABASE_URL"]
        result = urlparse(db_url)
        return psycopg2.connect(
            dbname=result.path[1:],
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port,
            sslmode="require",
            connect_timeout=CONN_TIMEOUT
        )

def release_connection(conn):
    if conn:
        try:
            if DB_POOL and not conn.closed:
                DB_POOL.putconn(conn)
            elif not conn.closed:
                conn.close()
        except Exception as e:
            print(f"⚠️ Release connection error: {e}")

def close_all_connections():
    global DB_POOL
    if DB_POOL:
        try:
            DB_POOL.closeall()
            print("✅ All connections closed")
        except Exception as e:
            print(f"⚠️ Close connections error: {e}")
        finally:
            DB_POOL = None

# --- Query Executor ---
@with_db_retry(max_retries=2)
def execute_query(query, params=None, fetch=False, fetchone=False, return_affected_rows=False):
    conn, cursor, result = None, None, None
    try:
        conn = get_connection()
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
        if conn: conn.rollback()
        st.error(f"❌ DB Error: {e}")
        print(f"Failed query: {query} | Params: {params}")
        raise
    finally:
        if cursor and not cursor.closed:
            cursor.close()
        if conn:
            release_connection(conn)

# --- Schema Initialization ---
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
        "CREATE INDEX IF NOT EXISTS idx_ticker_history_ticker ON ticker_history(ticker)",
        "CREATE INDEX IF NOT EXISTS idx_ticker_history_timestamp ON ticker_history(timestamp)",
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
        "CREATE INDEX IF NOT EXISTS idx_pump_history_ticker ON pump_history(ticker)",
        "CREATE INDEX IF NOT EXISTS idx_pump_history_timestamp ON pump_history(timestamp)"
    ]
    for q in queries:
        execute_query(q)

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
        WHERE ticker = %s
        ORDER BY timestamp DESC
        LIMIT %s
    """, (ticker, limit), fetch=True)

def save_pump_log(data):
    return execute_query("""
        INSERT INTO pump_history 
        (ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        data['ticker'],
        data['harga_sebelum'],
        data['harga_sekarang'],
        data['kenaikan_harga'],
        data['kenaikan_volume']
    ), return_affected_rows=True)

def get_pump_history(limit=50):
    return execute_query("""
        SELECT 
            ticker, harga_sebelum::numeric(18,8), harga_sekarang::numeric(18,8),
            kenaikan_harga::numeric(18,2), kenaikan_volume::numeric(18,2),
            timestamp::varchar(19)
        FROM pump_history
        ORDER BY timestamp DESC
        LIMIT %s
    """, (limit,), fetch=True)

def save_price_event_log(data):
    # Placeholder kalau nanti mau bikin tabel price_event_log
    pass

# --- Health Check ---
def check_db_health():
    try:
        result = execute_query("SELECT 1", fetchone=True)
        return result[0] == 1 if result else False
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
