import sqlite3
from datetime import datetime, timedelta
import pytz
import os

wib = pytz.timezone('Asia/Jakarta')
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, "database", "crypto.db")

def get_conn():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS ticker_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT, last REAL, vol_idr REAL, timestamp TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS pump_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT, harga_sebelum REAL, harga_sekarang REAL,
            kenaikan_harga REAL, kenaikan_volume REAL, timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_ticker_history(ticker, last, vol_idr):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO ticker_history (ticker, last, vol_idr, timestamp)
        VALUES (?, ?, ?, ?)
    """, (ticker, last, vol_idr, datetime.now(wib).strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit()
    conn.close()

def get_recent_price_volume(ticker, limit=4):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT last, vol_idr FROM ticker_history 
        WHERE ticker = ? ORDER BY id DESC LIMIT ?
    """, (ticker, limit))
    rows = c.fetchall()
    conn.close()
    return rows

def save_pump_log(data):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO pump_history (ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        data['ticker'], data['harga_sebelum'], data['harga_sekarang'],
        data['kenaikan_harga'], data['kenaikan_volume'], data['timestamp']
    ))
    conn.commit()
    conn.close()

def get_pump_history(limit=50):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT ticker, harga_sebelum, harga_sekarang, kenaikan_harga, kenaikan_volume, timestamp
        FROM pump_history ORDER BY id DESC LIMIT ?
    """, (limit,))
    rows = c.fetchall()
    conn.close()
    return rows