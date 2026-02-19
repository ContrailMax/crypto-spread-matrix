import streamlit as st
import pandas as pd
import numpy as np
import json
from google.oauth2 import service_account
from google.cloud import bigquery

# --- ตั้งค่าหน้าเว็บ ---
st.set_page_config(layout="wide", page_title="Crypto Arbitrage Matrix")
st.title("🚀 Crypto Arbitrage Matrix")

# --- 1. ฟังก์ชันเชื่อมต่อ BigQuery ---
@st.cache_resource
def get_bq_client():
    key_dict = json.loads(st.secrets["GCP_KEY"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    return bigquery.Client(credentials=credentials, project='project-2c68fafb-fc39-4b54-b6f')

# --- 2. ฟังก์ชันดึงข้อมูลแบบ "เหมา" (เพื่อความประหยัดและรวดเร็ว) ---
@st.cache_data(ttl=60) # อัปเดตข้อมูลใหม่ทุก 1 นาที
def load_recent_data():
    client = get_bq_client()
    # ดึงข้อมูลย้อนหลัง 1 วันมาทีเดียว จะได้ไม่ต้อง Query บ่อยๆ
    query = """
        SELECT * FROM `project-2c68fafb-fc39-4b54-b6f.spread_raw_data.price_logs`
        WHERE RunTimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        ORDER BY RunTimestamp DESC
    """
    df = client.query(query).to_dataframe()
    
    # แปลง RunTimestamp เป็น datetime ของ pandas ให้จัดการง่ายขึ้น
    df['RunTimestamp'] = pd.to_datetime(df['RunTimestamp'])
    
    # 🌟 สร้างตัวแปร price_usd ใน Python โดยไม่ต้องเปลืองเงิน GCP
    df['Price'] = pd.to_numeric(df['Price'])
    df['Fx'] = pd.to_numeric(df['Fx'])
    df['price_usd'] = df['Price'] / df['Fx']
    
    return df

# --- 3. ฟังก์ชันคำนวณ Spread (Pips) ---
def spread_pips(p1, p2):
    if pd.isna(p1) or pd.isna(p2) or p1 <= 0 or p2 <= 0:
        return np.nan
    denom = max(p1, p2)
    return ((p1 - p2) / denom) * 10000

# --- 4. ฟังก์ชันลงสี (Conditional Formatting) ---
def color_spread(val):
    if pd.isna(val): return ''
    if val > 0: return 'background-color: #d9f2d9; color: black;' # เขียว
    elif val < 0: return 'background-color: #f9d6d5; color: black;' # แดง
    else: return 'background-color: #eeeeee; color: black;' # เทา

# --- 5. เริ่มกระบวนการ UI และสร้าง Matrix ---
try:
    with st.spinner('Fetching latest data from BigQuery...'):
        df_all = load_recent_data()
    
    if df_all.empty:
        st.warning("⚠️ No data found in the last 24 hours.")
    else:
        # เตรียมคอลัมน์วันที่และเวลา สำหรับทำ Dropdown
        df_all['Date'] = df_all['RunTimestamp'].dt.date
        df_all['Time'] = df_all['RunTimestamp'].dt.strftime('%H:%M:%S')

        st.subheader("🗓️ Select Timestamp to View")
        
        # --- สร้าง 2 Dropdown (เลือกวัน แล้วค่อยเลือกเวลา) ---
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            available_dates = df_all['Date'].unique()
            selected_date = st.selectbox("1. Select Date", available_dates)
            
        with col2:
            # กรองเวลาให้เหลือเฉพาะของวันที่เลือก
            available_times = df_all[df_all['Date'] == selected_date]['Time'].unique()
            selected_time = st.selectbox("2. Select Time", available_times)
            
        with col3:
            st.write("") # เว้นบรรทัดให้ปุ่มตรงกัน
            st.write("")
            if st.button("🔄 Refresh Latest Data"):
                st.cache_data.clear()
                st.rerun()

        # กรองข้อมูลตาม วันและเวลา ที่ผู้ใช้เลือกจาก Dropdown
        df = df_all[(df_all['Date'] == selected_date) & (df_all['Time'] == selected_time)].copy()
        
        # ทำให้ตัวอักษร Side เป็นตัวพิมพ์ใหญ่ทั้งหมด (ASK, BID)
        df['Side'] = df['Side'].str.upper()
        
        # สร้าง Pivot Table สรุปราคา ASK/BID ของแต่ละ Exchange
        pivot_df = df.pivot_table(index='Exchange', columns='Side', values='price_usd', aggfunc='last')
        exchanges = sorted(pivot_df.index.tolist())

        # ฟังก์ชันช่วยสร้างตาราง
        def build_matrix(exchanges, pivot_df, row_side, col_side):
            mat = pd.DataFrame(index=exchanges, columns=exchanges)
            for ex_row in exchanges:
                for ex_col in exchanges:
                    if ex_row == ex_col:
                        mat.loc[ex_row, ex_col] = 0.0
                    else:
                        p1 = pivot_df.loc[ex_row, row_side] if row_side in pivot_df.columns and ex_row in pivot_df.index else np.nan
                        p2 = pivot_df.loc[ex_col, col_side] if col_side in pivot_df.columns and ex_col in pivot_df.index else np.nan
                        mat.loc[ex_row, ex_col] = spread_pips(p1, p2)
            return mat.astype(float)

        # --- 6. แสดงผลแยก 4 Tabs ---
        st.markdown("---")
        tab1, tab2, tab3, tab4 = st.tabs(["ASK-BID", "BID-ASK", "ASK-ASK", "BID-BID"])
        
        with tab1:
            st.markdown(f"**Matrix: ASK-BID** (Data from: {selected_date} {selected_time})")
            mat_ask_bid = build_matrix(exchanges, pivot_df, 'ASK', 'BID')
            st.dataframe(mat_ask_bid.style.map(color_spread).format("{:.2f}", na_rep=""), use_container_width=True)
            
        with tab2:
            st.markdown(f"**Matrix: BID-ASK** (Data from: {selected_date} {selected_time})")
            mat_bid_ask = build_matrix(exchanges, pivot_df, 'BID', 'ASK')
            st.dataframe(mat_bid_ask.style.map(color_spread).format("{:.2f}", na_rep=""), use_container_width=True)

        with tab3:
            st.markdown(f"**Matrix: ASK-ASK** (Data from: {selected_date} {selected_time})")
            mat_ask_ask = build_matrix(exchanges, pivot_df, 'ASK', 'ASK')
            st.dataframe(mat_ask_ask.style.map(color_spread).format("{:.2f}", na_rep=""), use_container_width=True)

        with tab4:
            st.markdown(f"**Matrix: BID-BID** (Data from: {selected_date} {selected_time})")
            mat_bid_bid = build_matrix(exchanges, pivot_df, 'BID', 'BID')
            st.dataframe(mat_bid_bid.style.map(color_spread).format("{:.2f}", na_rep=""), use_container_width=True)

except Exception as e:
    st.error(f"❌ An error occurred: {e}")
