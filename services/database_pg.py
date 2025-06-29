import psycopg2
from psycopg2 import pool
from urllib.parse import urlparse
import streamlit as st
import time
from functools import wraps

# --- Connection Pool Configuration ---
DB_POOL = None
MAX_CONN = 5
CONN_TIMEOUT = 5
RETRY_DELAY = 1

# --- Decorator Retry ---
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
                    if retry_count <= max_retries:
                        time.sleep(RETRY_DELAY * retry_count)
                        continue
                    raise
            raise last_error if last_error else Exception("Unknown DB error")
        return wrapper
    return decorator

# --- Connection Pool Management ---
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
        st.error(f"❌ DB Pool init failed: {str(e)}")
        DB_POOL = None

def get_connection():
    global DB_POOL
    if DB_POOL is None:
        init_connection_pool()
    try:
        return DB_POOL.getconn()
    except psycopg2.pool.PoolError as e:
        st.error(f"❌ DB connection failed: {e}")
        raise

def release_connection(conn):
    global DB_POOL
    if conn:
        try:
            if DB_POOL and not conn.closed:
                DB_POOL.putconn(conn)
            elif not conn.closed:
                conn.close()
        except:
            pass

def close_all_connections():
    global DB_POOL
    if DB_POOL:
        try:
            DB_POOL.closeall()
            print("✅ DB connections closed")
        except:
            pass
        DB_POOL = None

# --- Query Executor ---
@with_db_retry(max_retries=2)
def execute_query(query, params=None, fetch=False, fetchone=False, return_affected_rows=False):
    conn = None
    cursor = None
    result = None
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
        if conn:
            conn.rollback()
        st.error(f"❌ DB Error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)

# --- DB Schema Initialization ---
def init_db_schema():
    queries = [
        """
        CREATE TABLE IF NOT EXISTS ticker_history (
            id SERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            last NUMERIC(18,8) NOT NULL,
            vol_idr NUMERIC(18,2) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'unique_ticker_timestamp'
            ) THEN
                ALTER TABLE ticker_history
                ADD CONSTRAINT unique_ticker_timestamp UNIQUE (ticker, timestamp);
            END IF;
        END$$;
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_ticker_history_ticker ON ticker_history(ticker)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_ticker_history_timestamp ON ticker_history(timestamp)
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
        CREATE INDEX IF NOT EXISTS idx_pump_history_ticker ON pump_history(ticker)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_pump_history_timestamp ON pump_history(timestamp)
        """

        
    ]
    for query in queries:
        execute_query(query)

# --- CRUD Utilities ---
def save_ticker_history(ticker, last, vol_idr):
    execute_query(
        """
        INSERT INTO ticker_history (ticker, last, vol_idr)
        VALUES (%s, %s, %s)
        ON CONFLICT (ticker, timestamp) DO NOTHING
        """,
        (ticker, last, vol_idr)
    )

def get_recent_price_volume(ticker, limit=5):
    results = execute_query(
        """
        SELECT last, vol_idr FROM ticker_history
        WHERE ticker = %s
        ORDER BY timestamp DESC
        LIMIT %s
        """,
        (ticker, limit),
        fetch=True
    )
    return results or []

def save_pump_log(data):
    execute_query(
        """
        INSERT INTO pump_history 
        (ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            data['ticker'],
            data['harga_sebelum'],
            data['harga_sekarang'],
            data['kenaikan_harga'],
            data['kenaikan_volume']
        )
    )

def get_pump_history(limit=50):
    results = execute_query(
        """
        SELECT ticker, harga_sebelum::numeric(18,8), harga_sekarang::numeric(18,8),
        kenaikan_harga::numeric(18,2), kenaikan_volume::numeric(18,2),
        timestamp::varchar(19)
        FROM pump_history
        ORDER BY timestamp DESC
        LIMIT %s
        """,
        (limit,),
        fetch=True
    )
    return results or []

def get_all_tickers():
    results = execute_query(
        """
        SELECT DISTINCT ticker FROM ticker_history
        WHERE timestamp > NOW() - INTERVAL '7 days'
        ORDER BY ticker
        """,
        fetch=True
    )
    return [r[0] for r in results] if results else []

# --- DB Health Check ---
def check_db_health():
    try:
        result = execute_query("SELECT 1", fetchone=True)
        return result[0] == 1 if result else False
    except:
        return False

# --- Auto Init at Import ---
if 'DB_INITIALIZED' not in st.session_state:
    try:
        init_connection_pool()
        init_db_schema()
        st.session_state.DB_INITIALIZED = True
    except Exception as e:
        st.error(f"❌ DB init error: {e}")
        close_all_connections()


@st.cache_data(ttl=60, show_spinner=False)
def get_price_history_since(ticker, since_date):
    """Ambil histori harga sejak tanggal tertentu"""
    try:
        results = execute_query(
            """
            SELECT last FROM ticker_history
            WHERE ticker = %s AND timestamp >= %s
            ORDER BY timestamp DESC
            """,
            (ticker, since_date),
            fetch=True
        )
        return results or []
    except Exception as e:
        st.error(f"❌ Error get_price_history_since: {e}")
        return []
    
@st.cache_data(ttl=300, show_spinner=False)
def get_last_30_daily_closes(ticker):
    """Ambil 30 harga penutupan harian terakhir"""
    try:
        results = execute_query(
            """
            SELECT last FROM (
                SELECT DISTINCT ON (DATE(timestamp)) 
                    DATE(timestamp) as tgl, 
                    last
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

@st.cache_data(ttl=60, show_spinner=False)
def get_last_n_closes(ticker, limit=30):
    """Ambil n harga close terakhir berdasarkan timestamp DESC"""
    try:
        results = execute_query(
            """
            SELECT last FROM ticker_history
            WHERE ticker = %s
            ORDER BY timestamp DESC
            LIMIT %s
            """,
            (ticker, limit),
            fetch=True
        )
        return [r[0] for r in results] if results else []
    except Exception as e:
        st.error(f"❌ Error get_last_n_closes: {e}")
        return []