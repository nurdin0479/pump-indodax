import requests
from datetime import datetime
import pytz
from services import database_pg
import streamlit as st

wib = pytz.timezone('Asia/Jakarta')

def fetch_indodax_data():
    url = "https://indodax.com/api/tickers"
    res = requests.get(url).json()['tickers']
    result = []
    for ticker, info in res.items():
        try:
            result.append({
                "ticker": ticker,
                "last": float(info["last"]),
                "vol_idr": float(info["vol_idr"])
            })
        except KeyError:
            continue
    return result

def is_valid_pump(ticker, price_threshold, volume_threshold, window=2):
    rows = database_pg.get_recent_price_volume(ticker, limit=window+1)
    if len(rows) < window+1:
        return False, None

    current_price, current_vol = rows[0]
    prev_price, prev_vol = rows[1]

    price_change = ((current_price - prev_price) / prev_price) * 100 if prev_price else 0
    volume_change = ((current_vol - prev_vol) / prev_vol) * 100 if prev_vol else 0

    if price_change >= price_threshold and volume_change >= volume_threshold:
        return True, {
            "ticker": ticker,
            "harga_sebelum": round(prev_price, 2),
            "harga_sekarang": round(current_price, 2),
            "kenaikan_harga": round(price_change, 2),
            "kenaikan_volume": round(volume_change, 2),
            "timestamp": datetime.now(wib).strftime('%Y-%m-%d %H:%M:%S')
        }
    else:
        return False, None

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{st.secrets['TELEGRAM_TOKEN']}/sendMessage"
    payload = {
        "chat_id": st.secrets["TELEGRAM_CHAT_ID"],
        "text": message
    }
    requests.post(url, data=payload)
