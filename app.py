import streamlit as st
import duckdb
import pandas as pd
import time

st.set_page_config(page_title="Streaming Wars HQ", layout="wide")
st.title("🎬 Franchise Valuation Engine")

try:
    with duckdb.connect('franchise_data.db', read_only=True) as db:
        
        # 1. Total Data Volume (For your Report)
        count = db.execute("SELECT count(*) FROM franchise_metrics").fetchone()[0]
        st.caption(f"💾 Database Volume: {count:,} records processed.")

        # 2. Historical Hype Data (Google Trends)
        history_df = db.execute("""
            SELECT 
                to_timestamp(timestamp) as time,
                title,
                hype_score
            FROM franchise_metrics
            WHERE active_watchers = 0 AND hype_score > 0 -- Filter for history rows
            ORDER BY timestamp ASC
        """).df()

        # 3. Live Data
        live_df = db.execute("""
            SELECT 
                to_timestamp(timestamp) as time,
                title,
                active_watchers
            FROM franchise_metrics
            WHERE active_watchers > 0
            ORDER BY timestamp ASC
        """).df()
        
        # 4. Snapshots for ROI
        latest_df = db.execute("""
            SELECT 
                title, 
                arg_max(brand_equity, timestamp) as votes,
                arg_max(cost_basis, timestamp) as seasons
            FROM franchise_metrics
            GROUP BY title
        """).df()
    
except Exception as e:
    st.warning(f"Waiting for data... ({e})")
    count = 0
    history_df = pd.DataFrame()
    live_df = pd.DataFrame()
    latest_df = pd.DataFrame()

# --- VISUALIZATIONS ---

if count > 0:
    # Row 1: The Massive History Chart (50k+ points)
    st.subheader(f"📅 5-Year Search Volume History ({len(history_df)} records)")
    st.line_chart(history_df, x='time', y='hype_score', color='title')

    # Row 2: Live Stats
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📈 Real-Time Viewers (Trakt)")
        if not live_df.empty:
            st.line_chart(live_df, x='time', y='active_watchers', color='title')
        else:
            st.info("Waiting for live stream...")

    with col2:
        st.subheader("💰 ROI Efficiency (Votes / Season)")
        if not latest_df.empty:
            latest_df['roi'] = latest_df['votes'] / latest_df['seasons']
            st.bar_chart(latest_df.set_index('title')['roi'])

else:
    st.info("Start producer.py to generate 50k records!")

time.sleep(5)
st.rerun()