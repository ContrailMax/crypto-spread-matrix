import streamlit as st
import pandas as pd
import numpy as np
import json
import plotly.express as px
from google.oauth2 import service_account
from google.cloud import bigquery

# --- ตั้งค่าหน้าเว็บ ---
st.set_page_config(layout="wide", page_title="Crypto Arbitrage Dashboard")
st.title("🚀 Crypto Arbitrage Dashboard")

# --- 1. ฟังก์ชันเชื่อมต่อ BigQuery ---
@st.cache_resource
def get_bq_client():
    key_dict = json.loads(st.secrets["GCP_KEY"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    return bigquery.Client(credentials=credentials, project='project-2c68fafb-fc39-4b54-b6f')

# --- 2. ฟังก์ชันดึงข้อมูลแบบ "เหมา" ย้อนหลัง 3 วัน ---
@st.cache_data(ttl=60) 
def load_recent_data():
    client = get_bq_client()
    query = """
        SELECT * FROM `project-2c68fafb-fc39-4b54-b6f.spread_raw_data.price_logs`
        WHERE RunTimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
        ORDER BY RunTimestamp DESC
    """
    df = client.query(query).to_dataframe()
    df['RunTimestamp'] = pd.to_datetime(df['RunTimestamp'], utc=True).dt.tz_convert('Asia/Bangkok').dt.tz_localize(None)
    df['Price'] = pd.to_numeric(df['Price'])
    df['Fx'] = pd.to_numeric(df['Fx'])
    df['price_usd'] = df['Price'] / df['Fx']
    df['Side'] = df['Side'].str.upper()
    return df

# --- 3. ฟังก์ชันคำนวณ Spread (Pips) ---
def spread_pips(p1, p2):
    if pd.isna(p1) or pd.isna(p2) or p1 <= 0 or p2 <= 0:
        return np.nan
    denom = max(p1, p2)
    return ((p1 - p2) / denom) * 10000

# --- 4. ฟังก์ชันลงสี Matrix ---
def color_spread(val):
    if pd.isna(val): return ''
    if val > 0: return 'background-color: #d9f2d9; color: black;' 
    elif val < 0: return 'background-color: #f9d6d5; color: black;' 
    else: return 'background-color: #eeeeee; color: black;' 

# --- 5. เริ่มกระบวนการ UI ---
try:
    with st.spinner('Fetching latest data from BigQuery...'):
        df_all = load_recent_data()
    
    if df_all.empty:
        st.warning("⚠️ No data found in the last 3 days.")
    else:
        df_all['Date'] = df_all['RunTimestamp'].dt.date
        df_all['Time'] = df_all['RunTimestamp'].dt.strftime('%H:%M:%S')

        # 🌟 กลับมาใช้ Tabs แนวนอนแบบเดิม
        main_tab1, main_tab2, main_tab3 = st.tabs(["📊 Spread Matrix", "📈 Historical Trend", "🔀 4-Leg Arbitrage"])

        # ==========================================
        # 🟢 TAB 1: SPREAD MATRIX
        # ==========================================
        with main_tab1:
            st.subheader("🗓️ Select Coin & Timestamp")
            
            coin_list = sorted(df_all['Coin'].unique())
            col_c, col1, col2, col3 = st.columns([1, 1, 1, 1.5])
            with col_c:
                selected_coin = st.selectbox("🪙 Select Coin", coin_list, key="matrix_coin")
            
            df_coin = df_all[df_all['Coin'] == selected_coin]
            
            with col1:
                available_dates = df_coin['Date'].unique()
                selected_date = st.selectbox("1. Select Date", available_dates, key="matrix_date")
            with col2:
                available_times = df_coin[df_coin['Date'] == selected_date]['Time'].unique()
                selected_time = st.selectbox("2. Select Time", available_times, key="matrix_time")
            with col3:
                st.write("")
                st.write("")
                if st.button("🔄 Refresh Latest Data"):
                    st.cache_data.clear()
                    st.rerun()

            df_matrix = df_coin[(df_coin['Date'] == selected_date) & (df_coin['Time'] == selected_time)].copy()
            pivot_df = df_matrix.pivot_table(index='Exchange', columns='Side', values='price_usd', aggfunc='last')
            exchanges = sorted(pivot_df.index.tolist())

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

            st.markdown("---")
            sub_tab1, sub_tab2, sub_tab3, sub_tab4 = st.tabs(["ASK-BID", "BID-ASK", "ASK-ASK", "BID-BID"])
            with sub_tab1:
                mat_ask_bid = build_matrix(exchanges, pivot_df, 'ASK', 'BID')
                st.dataframe(mat_ask_bid.style.map(color_spread).format("{:.2f}", na_rep=""), use_container_width=True)
            with sub_tab2:
                mat_bid_ask = build_matrix(exchanges, pivot_df, 'BID', 'ASK')
                st.dataframe(mat_bid_ask.style.map(color_spread).format("{:.2f}", na_rep=""), use_container_width=True)
            with sub_tab3:
                mat_ask_ask = build_matrix(exchanges, pivot_df, 'ASK', 'ASK')
                st.dataframe(mat_ask_ask.style.map(color_spread).format("{:.2f}", na_rep=""), use_container_width=True)
            with sub_tab4:
                mat_bid_bid = build_matrix(exchanges, pivot_df, 'BID', 'BID')
                st.dataframe(mat_bid_bid.style.map(color_spread).format("{:.2f}", na_rep=""), use_container_width=True)


        # ==========================================
        # 🔵 TAB 2: HISTORICAL TREND (GRAPH)
        # ==========================================
        with main_tab2:
            st.subheader("Custom Historical Spread Graphs")
            
            if 'graph_configs' not in st.session_state:
                st.session_state.graph_configs = []

            ex_list = sorted(df_all['Exchange'].unique())
            
            with st.expander("➕ Create New Graph (Click to expand)", expanded=True):
                c1, c2, c3, c4, c5 = st.columns(5)
                exA = c1.selectbox("Exchange A", ex_list, key="new_exA")
                exB = c2.selectbox("Exchange B", ex_list, index=(1 if len(ex_list) > 1 else 0), key="new_exB")
                coin_sel = c3.selectbox("Coin", coin_list, key="new_coin")
                
                # 🌟 ทิศทาง A -> B อย่างเดียวตามที่คุณต้องการ
                dir_sel = c4.selectbox("Side Comparison", ["Ask -> Bid", "Bid -> Ask", "Ask -> Ask", "Bid -> Bid"], key="new_dir")
                
                c5.write("")
                c5.write("")
                if c5.button("📊 Add Graph", type="primary"):
                    st.session_state.graph_configs.append({
                        'exA': exA, 'exB': exB, 'coin': coin_sel, 'direction': dir_sel
                    })
                    st.rerun()

            if not st.session_state.graph_configs:
                st.info("👆 Please select options above and click 'Add Graph' to generate a visualization.")
            
            for i, g_config in enumerate(st.session_state.graph_configs):
                st.markdown("---")
                
                h_col1, h_col2 = st.columns([9, 1])
                h_col1.markdown(f"#### 📉 {g_config['exA']} ➡️ {g_config['exB']} | Coin: {g_config['coin']} | {g_config['direction']}")
                if h_col2.button("❌ Remove", key=f"del_{i}"):
                    st.session_state.graph_configs.pop(i)
                    st.rerun()

                min_date = df_all['RunTimestamp'].dt.date.min()
                max_date = df_all['RunTimestamp'].dt.date.max()

                f_col1, f_col2, f_col3, f_col4 = st.columns(4)
                start_date = f_col1.date_input("Start Date", min_date, key=f"sd_{i}")
                start_time = f_col2.time_input("Start Time", pd.to_datetime("00:00").time(), key=f"st_{i}")
                end_date = f_col3.date_input("End Date", max_date, key=f"ed_{i}")
                end_time = f_col4.time_input("End Time", pd.to_datetime("23:59:59").time(), key=f"et_{i}")

                start_dt = pd.to_datetime(f"{start_date} {start_time}")
                end_dt = pd.to_datetime(f"{end_date} {end_time}")

                df_g = df_all[
                    (df_all['Coin'] == g_config['coin']) &
                    (df_all['Exchange'].isin([g_config['exA'], g_config['exB']])) &
                    (df_all['RunTimestamp'] >= start_dt) &
                    (df_all['RunTimestamp'] <= end_dt)
                ]

                if df_g.empty:
                    st.warning(f"No data available for the selected time range ({start_dt} to {end_dt}).")
                else:
                    pivot_chart = df_g.pivot_table(index='RunTimestamp', columns=['Exchange', 'Side'], values='price_usd')
                    trend_data = pd.DataFrame(index=pivot_chart.index)
                    
                    def calc_series(s1, s2):
                        return ((s1 - s2) / np.maximum(s1, s2)) * 10000

                    chart_label = f"Spread (pips)"
                    
                    if g_config['direction'] == "Ask -> Bid":
                        if (g_config['exA'], 'ASK') in pivot_chart.columns and (g_config['exB'], 'BID') in pivot_chart.columns:
                            trend_data[chart_label] = calc_series(pivot_chart[(g_config['exA'], 'ASK')], pivot_chart[(g_config['exB'], 'BID')])
                    elif g_config['direction'] == "Bid -> Ask":
                        if (g_config['exA'], 'BID') in pivot_chart.columns and (g_config['exB'], 'ASK') in pivot_chart.columns:
                            trend_data[chart_label] = calc_series(pivot_chart[(g_config['exA'], 'BID')], pivot_chart[(g_config['exB'], 'ASK')])
                    elif g_config['direction'] == "Ask -> Ask":
                        if (g_config['exA'], 'ASK') in pivot_chart.columns and (g_config['exB'], 'ASK') in pivot_chart.columns:
                            trend_data[chart_label] = calc_series(pivot_chart[(g_config['exA'], 'ASK')], pivot_chart[(g_config['exB'], 'ASK')])
                    elif g_config['direction'] == "Bid -> Bid":
                        if (g_config['exA'], 'BID') in pivot_chart.columns and (g_config['exB'], 'BID') in pivot_chart.columns:
                            trend_data[chart_label] = calc_series(pivot_chart[(g_config['exA'], 'BID')], pivot_chart[(g_config['exB'], 'BID')])

                    if not trend_data.empty and chart_label in trend_data.columns:
                        trend_data = trend_data.reset_index()
                        fig = px.line(
                            trend_data, 
                            x='RunTimestamp', 
                            y=chart_label,
                            labels={"value": "Spread (pips)", "RunTimestamp": "Date & Time (UTC+7)"}
                        )
                        fig.update_layout(
                            title=f"A: {g_config['exA']} ➡️ B: {g_config['exB']} ({g_config['direction']})",
                            hovermode="x unified"
                        )
                        fig.add_hline(y=0, line_dash="dash", line_color="red", opacity=0.7)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("⚠️ Missing data for this specific pair/side in the selected time range.")


        # ==========================================
        # 🟡 TAB 3: 4-LEG ARBITRAGE
        # ==========================================
        with main_tab3:
            st.header("🔀 4-Leg Arbitrage (Cross-Exchange Triangular)")
            # 🌟 ลบ Planned Features ออกตามคำขอ
            st.info("🚧 Coming Soon...")

except Exception as e:
    st.error(f"❌ An error occurred: {e}")
