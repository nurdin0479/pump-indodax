import psycopg2
from psycopg2 import pool
from urllib.parse import urlparse
import streamlit as st
import time
from functools import wraps

# --- Connection Pool Configuration ---
DB_POOL = None
MAX_CONN = 5  # Conservative limit for Aiven Free (20 max)
CONN_TIMEOUT = 5  # seconds for connection establishment
RETRY_DELAY = 1  # base delay for retries in seconds

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
                    if retry_count <= max_retries:
                        time.sleep(RETRY_DELAY * retry_count)
                        continue
                    raise
            raise last_error if last_error else Exception("Unknown database error")
        return wrapper
    return decorator

# --- Connection Pool Management ---
def init_connection_pool():
    """Initialize PostgreSQL connection pool with conservative settings"""
    global DB_POOL
    
    if DB_POOL is not None:
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
            connect_timeout=CONN_TIMEOUT,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        print("✅ Database connection pool initialized")
    except Exception as e:
        st.error(f"❌ Failed to initialize connection pool: {str(e)}")
        DB_POOL = None

def get_connection():
    """Get a database connection with fallback mechanism"""
    global DB_POOL
    
    if DB_POOL is None:
        init_connection_pool()
    
    try:
        # Removed timeout parameter from getconn()
        return DB_POOL.getconn()
    except psycopg2.pool.PoolError as e:
        print(f"⚠️ Connection pool exhausted, creating direct connection: {str(e)}")
        try:
            # Fallback to direct connection if pool is exhausted
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
        except psycopg2.Error as e:
            st.error(f"❌ Failed to establish database connection: {str(e)}")
            raise

def release_connection(conn):
    """Release connection back to pool or close it"""
    global DB_POOL
    
    if conn is None:
        return
    
    try:
        if DB_POOL and not conn.closed:
            DB_POOL.putconn(conn)
        elif not conn.closed:
            conn.close()
    except Exception as e:
        print(f"⚠️ Error releasing connection: {str(e)}")
        try:
            if not conn.closed:
                conn.close()
        except:
            pass

def close_all_connections():
    """Close all connections in the pool"""
    global DB_POOL
    
    if DB_POOL:
        try:
            DB_POOL.closeall()
            print("✅ All database connections closed")
        except Exception as e:
            print(f"⚠️ Error closing connections: {str(e)}")
        finally:
            DB_POOL = None

# --- Database Operations ---
@with_db_retry(max_retries=2)
def execute_query(query, params=None, fetch=False, fetchone=False, return_affected_rows=False):
    """
    Execute a database query with automatic connection handling
    
    Args:
        query: SQL query string
        params: Parameters for the query
        fetch: Whether to fetch all results
        fetchone: Whether to fetch one result
        return_affected_rows: Return number of affected rows for INSERT/UPDATE/DELETE
    
    Returns:
        Query results or None if error
    """
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
        st.error(f"❌ Database error: {str(e)}")
        print(f"Failed query: {query}\nParams: {params}")
        raise
    finally:
        if cursor and not cursor.closed:
            cursor.close()
        if conn:
            release_connection(conn)

# --- Database Schema Initialization ---
def init_db_schema():
    """Initialize database tables if they don't exist"""
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
        try:
            execute_query(query)
        except Exception as e:
            st.error(f"❌ Failed to initialize database schema: {str(e)}")
            raise

# --- CRUD Operations with Caching ---
@st.cache_data(ttl=60, show_spinner=False)
def save_ticker_history(ticker, last, vol_idr):
    """Save ticker data with deduplication"""
    execute_query(
        """
        INSERT INTO ticker_history (ticker, last, vol_idr)
        VALUES (%s, %s, %s)
        ON CONFLICT (ticker, timestamp) DO NOTHING
        """,
        (ticker, last, vol_idr)
    )

@st.cache_data(ttl=300, show_spinner=False)
def get_recent_price_volume(ticker, limit=4):
    """Get recent price and volume data for a ticker"""
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

@st.cache_data(ttl=10, show_spinner=False)
def save_pump_log(data):
    """Save pump detection log"""
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
        ),
        return_affected_rows=True
    )

@st.cache_data(ttl=60, show_spinner=False)
def get_pump_history(limit=50):
    """Get recent pump history"""
    results = execute_query(
        """
        SELECT 
            ticker, 
            harga_sebelum::numeric(18,8),
            harga_sekarang::numeric(18,8),
            kenaikan_harga::numeric(18,2),
            kenaikan_volume::numeric(18,2),
            timestamp::varchar(19)
        FROM pump_history
        ORDER BY timestamp DESC
        LIMIT %s
        """,
        (limit,),
        fetch=True
    )
    return results or []

@st.cache_data(ttl=600, show_spinner=False)
def get_all_tickers():
    """Get all unique tickers from recent data"""
    results = execute_query(
        """
        SELECT DISTINCT ticker FROM ticker_history
        WHERE timestamp > NOW() - INTERVAL '7 days'
        ORDER BY ticker
        """,
        fetch=True
    )
    return [r[0] for r in results] if results else []

@st.cache_data(ttl=300, show_spinner=False)
def get_price_history_since(ticker, since_date):
    """Get price history since specific date"""
    results = execute_query(
        """
        SELECT last FROM ticker_history
        WHERE ticker = %s AND timestamp >= %s
        ORDER BY timestamp DESC
        """,
        (ticker, since_date),
        fetch=True
    )
    return [r[0] for r in results] if results else []

@st.cache_data(ttl=3600, show_spinner=False)
def get_last_30_daily_closes(ticker):
    """Get last 30 daily closes for a ticker"""
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

# --- Database Health Check ---
def check_db_health():
    """Check if database is responsive"""
    try:
        result = execute_query("SELECT 1", fetchone=True)
        return result[0] == 1 if result else False
    except:
        return False

def get_pool_status():
    """Get connection pool status"""
    if DB_POOL:
        return {
            'min_connections': DB_POOL.minconn,
            'max_connections': DB_POOL.maxconn,
            'connections_in_use': len(DB_POOL._used),
            'connections_available': len(DB_POOL._rused)
        }
    return None

# --- Initialize when imported ---
if 'DB_INITIALIZED' not in st.session_state:
    try:
        init_connection_pool()
        init_db_schema()
        st.session_state.DB_INITIALIZED = True
    except Exception as e:
        st.error(f"❌ Failed to initialize database: {str(e)}")
        close_all_connections()