import streamlit as st
import pandas as pd
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
import sys
import os

# Add services directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'services')))

try:
    from services import database_pg, detector
except ImportError as e:
    st.error(f"❌ Failed to import required modules: {str(e)}")
    st.stop()

# --- App Initialization ---
def initialize_database():
    """Initialize database connection with error handling"""
    try:
        if not database_pg.init_connection_pool():
            st.error("Failed to initialize database connection pool")
            return False
        
        if not database_pg.init_db_schema():
            st.error("Failed to initialize database schema")
            return False
            
        return True
    except Exception as e:
        st.error(f"❌ Database initialization failed: {str(e)}")
        return False

# --- Main App ---
def main():
    st.set_page_config(
        page_title="Pump Detector Pro Indodax", 
        layout="wide",
        page_icon="🚨"
    )
    
    st.title("🚨 Tools Trading Profesional NBF SOFT")
    
    # Initialize database
    if not initialize_database():
        st.error("Application cannot start due to database initialization failure")
        st.stop()
    
    # Sidebar controls
    with st.sidebar:
        st.header("⚙️ Configuration")
        preset = st.radio(
            "🎛️ Preset Sensitivity", 
            ["Custom", "Aggressive", "Moderate", "Safe"], 
            index=0
        )
        
        # Parameter presets
        if preset == "Aggressive":
            interval = 3
            price_threshold = 1.0
            volume_threshold = 30.0
            price_delta = 1.0
            spike_factor = 1.5
        elif preset == "Moderate":
            interval = 3
            price_threshold = 1.5
            volume_threshold = 50.0
            price_delta = 1.0
            spike_factor = 1.7
        elif preset == "Safe":
            interval = 5
            price_threshold = 2.0
            volume_threshold = 80.0
            price_delta = 1.0
            spike_factor = 2.0
        else:  # Custom
            interval = st.selectbox("⏱️ Interval Refresh (detik)", [3, 5, 10], index=0)
            price_threshold = st.slider("📈 Threshold Harga (%)", 0.5, 5.0, 1.5, 0.1)
            volume_threshold = st.slider("📊 Threshold Volume (%)", 10.0, 500.0, 50.0, 5.0)
            price_delta = st.slider("📈 Price Delta (%)", 0.5, 5.0, 1.0, 0.1)
            spike_factor = st.slider("📊 Spike Factor Volume (x)", 1.0, 5.0, 1.5, 0.1)
    
    # Auto refresh
    st_autorefresh(interval=interval * 1000, key="data_refresh")
    
    # Main content
    st.header("📊 Monitoring harga realtime Indodax")
    
    try:
        # Fetch and process data
        data = detector.fetch_indodax_data()
        detected_pumps = []
        
        with st.spinner("Memproses data..."):
            for d in data:
                ticker = d['ticker']
                last = d['last']
                vol_idr = d['vol_idr']

                # Save history
                database_pg.save_ticker_history(ticker, last, vol_idr)

                # Detect pump
                is_pump, result = detector.is_valid_pump(
                    ticker,
                    price_threshold,
                    volume_threshold,
                    window=5,
                    min_consecutive_up=3,
                    price_delta=price_delta,
                    spike_factor=spike_factor
                )

                if is_pump:
                    detector.send_telegram_message(
                        f"🚨 PUMP DETECTED {result['ticker'].upper()}\n"
                        f"Harga: {result['harga_sebelum']} ➡️ {result['harga_sekarang']} (+{result['kenaikan_harga']:.2f}%)\n"
                        f"Volume: +{result['kenaikan_volume']:.2f}%\n"
                        f"Jam: {result['timestamp']}"
                    )
                    detected_pumps.append(result)
        
        # Display results
        if detected_pumps:
            st.subheader("📈 Pump Terdeteksi Saat Ini")
            st.dataframe(
                pd.DataFrame(detected_pumps),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("🔍 Tidak ada pump yang terdeteksi")
            
    except Exception as e:
        st.error(f"❌ Error saat memproses data: {str(e)}")
    
    st.write(f"🕒 Update terakhir: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} WIB")

if __name__ == "__main__":
    main()