import requests
from datetime import datetime
import pytz
from services import database_pg
import streamlit as st

# Set timezone WIB
wib = pytz.timezone('Asia/Jakarta')

@st.cache_data(ttl=5)
def fetch_indodax_data():
    """Ambil data ticker Indodax, di-cache selama 5 detik."""
    url = "https://indodax.com/api/tickers"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # error kalau status code != 200

        data = response.json()

        if 'tickers' not in data:
            st.error("❌ Response API Indodax tidak berisi key 'tickers'.")
            return []

        result = []
        for ticker, info in data['tickers'].items():
            try:
                result.append({
                    "ticker": ticker,
                    "last": float(info["last"]),
                    "vol_idr": float(info["vol_idr"])
                })
            except (KeyError, ValueError):
                continue  # skip kalau data error

        return result

    except requests.RequestException as e:
        st.error(f"❌ Gagal fetch data dari Indodax API: {e}")
        return []
    except ValueError:
        st.error("❌ Error parsing JSON dari API.")
        return []

def is_valid_pump(ticker, price_threshold, volume_threshold, window=3):
    # Ambil data 4 harga & volume terakhir
    rows = database_pg.get_recent_price_volume(ticker, limit=window+1)
    if len(rows) < window+1:
        return False, None

    prices = [row[0] for row in rows]
    volumes = [row[1] for row in rows]

    # Validasi harga naik 3x berturut-turut
    if not (prices[0] > prices[1] > prices[2] > prices[3]):
        return False, None

    # Hitung kenaikan harga & volume
    price_change = ((prices[0] - prices[3]) / prices[3]) * 100 if prices[3] else 0
    volume_change = ((volumes[0] - volumes[3]) / volumes[3]) * 100 if volumes[3] else 0

    # Validasi threshold pump
    if price_change >= price_threshold and volume_change >= volume_threshold:
        timestamp = datetime.now(wib).strftime('%Y-%m-%d %H:%M:%S')

        data = {
            "ticker": ticker,
            "harga_sebelum": round(prices[3], 2),
            "harga_sekarang": round(prices[0], 2),
            "kenaikan_harga": round(price_change, 2),
            "kenaikan_volume": round(volume_change, 2),
            "timestamp": timestamp
        }

        # Simpan log pump ke database
        database_pg.save_pump_log(data)

        return True, data  # <-- HANYA kalau pump terdeteksi

    return False, None  # <-- Kalau tidak valid



def send_telegram_message(message):
    try:
        url = f"https://api.telegram.org/bot{st.secrets['TELEGRAM_TOKEN']}/sendMessage"
        payload = {
            "chat_id": st.secrets["TELEGRAM_CHAT_ID"],
            "text": message
        }
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        st.error(f"❌ Gagal kirim pesan Telegram: {e}")
