import streamlit as st
import pandas as pd
import redis
import json
import time
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

st.set_page_config(page_title="Streaming Wars HQ", layout="wide")
st.title("🎬 Franchise Valuation Engine (Redis Edition)")

# --- 1. FETCH DATA FROM REDIS ---
@st.cache_data(ttl=5) # Cache for 5 seconds to prevent spamming Redis
def load_data():
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        # Fetch ALL data from the list (0 to -1 means everything)
        raw_data = r.lrange("franchise_data", 0, -1)
        
        # Convert bytes to JSON
        records = [json.loads(x) for x in raw_data]
        
        if not records: return pd.DataFrame()
        
        df = pd.DataFrame(records)
        df['time'] = pd.to_datetime(df['timestamp'], unit='s')
        return df
    except Exception as e:
        st.error(f"Redis Error: {e}")
        return pd.DataFrame()

df = load_data()

if not df.empty:
    st.caption(f"💾 Database Volume (Redis): {len(df):,} records processed.")

    # --- 2. FILTERING ---
    all_shows = sorted(df['title'].unique())
    selected_shows = st.multiselect(
        "Select Franchises:",
        options=all_shows,
        default=all_shows[:3] if all_shows else []
    )
    
    if not selected_shows: st.stop()
    
    # Filter Data
    filtered_df = df[df['title'].isin(selected_shows)]
    
    # --- 3. PREPARE DATASETS (Replacing SQL) ---
    
    # A. History (Hype Only)
    history_df = filtered_df[
        (filtered_df['active_watchers'] == 0) & 
        (filtered_df['hype_score'] > 0)
    ].sort_values('time')

    # B. ROI Snapshot (Latest value per show)
    # Sort by time and take the last one
    latest_df = filtered_df.sort_values('time').groupby('title').tail(1)
    
    # C. Viral Impact (Peak / Avg)
    pbr_data = []
    for show in selected_shows:
        show_data = filtered_df[filtered_df['title'] == show]
        peak = show_data['hype_score'].max()
        avg = show_data['hype_score'].mean()
        if avg > 0:
            pbr_data.append({'title': show, 'pbr_score': peak / avg})
    viral_df = pd.DataFrame(pbr_data)

    # D. ML Data (Daily -> Weekly Aggregation)
    # Resample to Weekly
    ml_df = filtered_df.set_index('time').groupby(['title', pd.Grouper(freq='W')]).agg({
        'hype_score': 'mean',
        'netflix_hours': 'max'
    }).reset_index()
    # Filter valid rows
    ml_df = ml_df[(ml_df['hype_score'] > 0) & (ml_df['netflix_hours'] > 0)]

    # --- 4. VISUALIZATIONS ---

    # Row 1: History
    st.subheader(f"📅 5-Year Search Volume History")
    st.line_chart(history_df, x='time', y='hype_score', color='title')

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("💥 Viral Impact (PBR)")
        if not viral_df.empty:
            st.bar_chart(viral_df.set_index('title')['pbr_score'])
    with col2:
        st.subheader("💰 ROI Efficiency")
        latest_df['roi'] = latest_df['brand_equity'] / latest_df['cost_basis']
        st.bar_chart(latest_df.set_index('title')['roi'])

    # ML Section
    st.markdown("---")
    st.subheader(f"🤖 AI Analyst: Predicting Official Netflix Hours")
    if len(ml_df) > 5:
        X = ml_df[['hype_score']]
        y = ml_df['netflix_hours']
        
        model = LinearRegression()
        model.fit(X, y)
        predictions = model.predict(X)
        r2 = r2_score(y, predictions)
        coef = model.coef_[0]
        
        m1, m2, m3 = st.columns(3)
        m1.metric("Model Accuracy", f"{r2:.2f}")
        m2.metric("Hype Multiplier", f"{int(coef):,}")
        m3.metric("Training Weeks", f"{len(ml_df)}")
        
        chart_data = pd.DataFrame({
            'Week': ml_df['time'],
            'Actual': y,
            'Predicted': predictions,
            'Show': ml_df['title']
        })
        st.line_chart(chart_data, x='Week', y=['Actual', 'Predicted'], color=['#ffaa00', '#0000ff'])
    else:
        st.info("Not enough overlapping history for ML.")

else:
    st.info("Waiting for data... Ensure Producer & Consumer are running!")

time.sleep(5)
st.rerun()