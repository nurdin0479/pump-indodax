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
        response.raise_for_status()

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
                continue

        return result

    except requests.RequestException as e:
        st.error(f"❌ Gagal fetch data dari Indodax API: {e}")
        return []
    except ValueError:
        st.error("❌ Error parsing JSON dari API.")
        return []

def is_valid_pump(ticker, price_threshold, volume_threshold, window=5, min_consecutive_up=3):
    """Deteksi pump profesional berbasis MA dan tren harga"""
    rows = database_pg.get_recent_price_volume(ticker, limit=window)
    if len(rows) < window:
        return False, None

    prices = [row[0] for row in rows]
    volumes = [row[1] for row in rows]

    price_ma = sum(prices) / len(prices)
    volume_ma = sum(volumes) / len(volumes)

    consecutive_up = sum(1 for i in range(1, len(prices)) if prices[i] > prices[i-1])

    price_change = ((prices[-1] - prices[0]) / prices[0]) * 100 if prices[0] else 0
    volume_change = ((volumes[-1] - volumes[0]) / volumes[0]) * 100 if volumes[0] else 0

    timestamp = datetime.now(wib).strftime('%Y-%m-%d %H:%M:%S')

    data = {
        "ticker": ticker,
        "harga_sebelum": round(prices[0], 2),
        "harga_sekarang": round(prices[-1], 2),
        "kenaikan_harga": round(price_change, 2),
        "kenaikan_volume": round(volume_change, 2),
        "ma_harga": round(price_ma, 2),
        "ma_volume": round(volume_ma, 2),
        "consecutive_up": consecutive_up,
        "timestamp": timestamp
    }

    # Simpan log event ke price_event_log
    if consecutive_up >= 2 and (price_change >= 1.0 or volume_change >= 5.0):
        database_pg.save_price_event_log(data)

    # Validasi kondisi pump
    if (consecutive_up >= min_consecutive_up and
        price_change >= price_threshold and
        volume_change >= volume_threshold and
        prices[-1] > price_ma * 1.01 and
        volumes[-1] > volume_ma * 1.05):

        # Simpan pump ke log pump_history
        database_pg.save_pump_log(data)
        return True, data

    return False, None

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
