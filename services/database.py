import psycopg2
from datetime import datetime
import pytz

# Timezone WIB
wib = pytz.timezone('Asia/Jakarta')

# Konfigurasi koneksi PostgreSQL Aiven
DB_PARAMS = {
    'dbname': 'defaultdb',
    'user': 'avnadmin',
    'password': 'supersecretpassword',
    'host': 'pg-12345.aivencloud.com',
    'port': 22345,
    'sslmode': 'require'
}

def get_conn():
    return psycopg2.connect(**DB_PARAMS)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ticker_history (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(20),
            last FLOAT,
            vol_idr FLOAT,
            timestamp TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pump_history (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(20),
            harga_sebelum FLOAT,
            harga_sekarang FLOAT,
            kenaikan_harga FLOAT,
            kenaikan_volume FLOAT,
            timestamp TIMESTAMP
        )
    """)

    conn.commit()
    cur.close()
    conn.close()

def save_ticker_history(ticker, last, vol_idr):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO ticker_history (ticker, last, vol_idr, timestamp)
        VALUES (%s, %s, %s, %s)
    """, (ticker, last, vol_idr, datetime.now(wib)))

    conn.commit()
    cur.close()
    conn.close()

def get_recent_price_volume(ticker, limit=4):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT last, vol_idr FROM ticker_history
        WHERE ticker = %s ORDER BY id DESC LIMIT %s
    """, (ticker, limit))

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def save_pump_log(data):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO pump_history 
        (ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        data['ticker'], data['harga_sebelum'], data['harga_sekarang'],
        data['kenaikan_harga'], data['kenaikan_volume'], data['timestamp']
    ))

    conn.commit()
    cur.close()
    conn.close()

def get_pump_history(limit=50):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume, timestamp
        FROM pump_history ORDER BY id DESC LIMIT %s
    """, (limit,))

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
