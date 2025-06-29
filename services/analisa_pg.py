import streamlit as st
import pandas as pd
from services import database_pg

# Inisialisasi pool saat import pertama
database_pg.init_connection_pool()

def get_all_tickers():
    try:
        conn = database_pg.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT ticker FROM ticker_history ORDER BY ticker")
        rows = cur.fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        st.error(f"❌ Error get_all_tickers: {e}")
        return []
    finally:
        cur.close()
        database_pg.release_connection(conn)

def get_last_30_daily_closes(ticker):
    try:
        conn = database_pg.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT last FROM (
                SELECT DISTINCT ON (DATE(timestamp)) DATE(timestamp) as tgl, last
                FROM ticker_history
                WHERE ticker = %s
                ORDER BY DATE(timestamp) DESC, timestamp DESC
            ) AS daily_prices
            ORDER BY tgl DESC
            LIMIT 30
        """, (ticker,))
        rows = cur.fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        st.error(f"❌ Error get_last_30_daily_closes: {e}")
        return []
    finally:
        cur.close()
        database_pg.release_connection(conn)

def get_last_n_closes(ticker, n):
    try:
        conn = database_pg.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT last FROM ticker_history
            WHERE ticker = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (ticker, n))
        rows = cur.fetchall()
        return [r[0] for r in rows][::-1]  # dari yang lama ke terbaru
    except Exception as e:
        st.error(f"❌ Error get_last_n_closes: {e}")
        return []
    finally:
        cur.close()
        database_pg.release_connection(conn)

def get_full_price_data(ticker):
    try:
        conn = database_pg.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT timestamp, last FROM ticker_history
            WHERE ticker = %s
            ORDER BY timestamp ASC
        """, (ticker,))
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=['timestamp', 'close'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        return df
    except Exception as e:
        st.error(f"❌ Error get_full_price_data: {e}")
        return pd.DataFrame()
    finally:
        cur.close()
        database_pg.release_connection(conn)
