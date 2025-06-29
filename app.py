import streamlit as st
import pandas as pd
from datetime import datetime
from services import database_pg, detector
from streamlit_autorefresh import st_autorefresh

# --- App Initialization ---
def initialize_app():
    """Initialize all application components"""
    try:
        # Initialize database connection and schema
        database_pg.init_connection_pool()
        database_pg.init_db_schema()
        
        # Page configuration
        st.set_page_config(
            page_title="Pump Detector Pro Indodax", 
            layout="wide",
            page_icon="🚨"
        )
        
        return True
    except Exception as e:
        st.error(f"❌ Failed to initialize application: {str(e)}")
        return False

# --- Main App Function ---
def run_pump_detector():
    st.title("🚨 Tools Trading Profesional NBF SOFT")
    
    # Sidebar controls
    with st.sidebar:
        st.header("⚙️ Configuration")
        preset = st.radio(
            "🎛️ Preset Sensitivity", 
            ["Custom", "Aggressive", "Moderate", "Safe"], 
            index=0
        )
        
        # Default parameters based on preset
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
            interval = st.selectbox(
                "⏱️ Interval Refresh (detik)", 
                [3, 5, 10], 
                index=0
            )
            price_threshold = st.slider(
                "📈 Threshold Harga (%)", 
                0.5, 5.0, 1.5, 0.1
            )
            volume_threshold = st.slider(
                "📊 Threshold Volume (%)", 
                10.0, 500.0, 50.0, 5.0
            )
            price_delta = st.slider(
                "📈 Price Delta (%)", 
                0.5, 5.0, 1.0, 0.1
            )
            spike_factor = st.slider(
                "📊 Spike Factor Volume (x)", 
                1.0, 5.0, 1.5, 0.1
            )
    
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
                    # Send Telegram notification
                    detector.send_telegram_message(
                        f"🚨 PUMP DETECTED {result['ticker'].upper()}\n"
                        f"Harga: {result['harga_sebelum']} ➡️ {result['harga_sekarang']} (+{result['kenaikan_harga']:.2f}%)\n"
                        f"Volume: +{result['kenaikan_volume']:.2f}%\n"
                        f"MA Harga: {result['ma_harga']}\n"
                        f"MA Volume: {result['ma_volume']}\n"
                        f"Consecutive Up: {result['consecutive_up']}x\n"
                        f"Jam: {result['timestamp']}"
                    )
                    detected_pumps.append(result)
        
        # Display results
        if detected_pumps:
            st.subheader("📈 Pump Terdeteksi Saat Ini")
            df = pd.DataFrame(detected_pumps)
            st.dataframe(
                df.sort_values('kenaikan_harga', ascending=False),
                use_container_width=True,
                hide_index=True
            )
            
            # Add download button
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Data Pump",
                data=csv,
                file_name=f"pump_detections_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime='text/csv'
            )
        else:
            st.info("🔍 Tidak ada pump yang terdeteksi dengan parameter saat ini")
            
    except Exception as e:
        st.error(f"❌ Error saat memproses data: {str(e)}")
    
    # Last update time
    st.write(f"🕒 Update terakhir: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} WIB")

# --- Run the app ---
if initialize_app():
    run_pump_detector()
else:
    st.error("Aplikasi tidak dapat dijalankan karena masalah inisialisasi. Silakan coba lagi nanti.")