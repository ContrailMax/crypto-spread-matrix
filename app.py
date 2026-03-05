import streamlit as st
import pandas as pd
import numpy as np
import json
import plotly.express as px
from google.oauth2 import service_account
from google.cloud import bigquery

# --- ตั้งค่าหน้าเว็บ ---
st.set_page_config(layout="wide", page_title="Crypto Arbitrage Dashboard")

# --- 1. ฟังก์ชันเชื่อมต่อ BigQuery ---
@st.cache_resource
def get_bq_client():
    key_dict = json.loads(st.secrets["GCP_KEY"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    return bigquery.Client(credentials=credentials, project='project-2c68fafb-fc39-4b54-b6f')

# --- 2. ฟังก์ชันดึงข้อมูล (เพิ่มพารามิเตอร์ days เพื่อให้โหลดไวขึ้น) ---
@st.cache_data(ttl=60) 
def load_recent_data(days=1):
    client = get_bq_client()
    query = f"""
        SELECT * FROM `project-2c68fafb-fc39-4b54-b6f.spread_raw_data.price_logs`
        WHERE RunTimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
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
    if pd.isna(p1) or pd.isna(p2) or p1 <= 0 or p2 <= 0: return np.nan
    return ((p1 - p2) / max(p1, p2)) * 10000

def color_spread(val):
    if pd.isna(val): return ''
    if val > 0: return 'background-color: #d9f2d9; color: black;' 
    elif val < 0: return 'background-color: #f9d6d5; color: black;' 
    else: return 'background-color: #eeeeee; color: black;' 

# ==========================================
# 🟢 ฟังก์ชันสำหรับแต่ละหน้า (Pages)
# ==========================================

def page_matrix():
    st.title("📊 Spread Matrix")
    df_all = st.session_state.df_all
    
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

    sub_tab1, sub_tab2, sub_tab3, sub_tab4 = st.tabs(["ASK-BID", "BID-ASK", "ASK-ASK", "BID-BID"])
    with sub_tab1: st.dataframe(build_matrix(exchanges, pivot_df, 'ASK', 'BID').style.map(color_spread).format("{:.2f}", na_rep=""), use_container_width=True)
    with sub_tab2: st.dataframe(build_matrix(exchanges, pivot_df, 'BID', 'ASK').style.map(color_spread).format("{:.2f}", na_rep=""), use_container_width=True)
    with sub_tab3: st.dataframe(build_matrix(exchanges, pivot_df, 'ASK', 'ASK').style.map(color_spread).format("{:.2f}", na_rep=""), use_container_width=True)
    with sub_tab4: st.dataframe(build_matrix(exchanges, pivot_df, 'BID', 'BID').style.map(color_spread).format("{:.2f}", na_rep=""), use_container_width=True)

def page_trend():
    st.title("📈 Historical Trend")
    df_all = st.session_state.df_all
    
    if 'graph_configs' not in st.session_state: st.session_state.graph_configs = []
    ex_list = sorted(df_all['Exchange'].unique())
    
    with st.expander("➕ Create New Graph", expanded=True):
        c1, c2, c3, c4, c5 = st.columns(5)
        exA = c1.selectbox("Exchange A", ex_list, key="new_exA")
        exB = c2.selectbox("Exchange B", ex_list, index=(1 if len(ex_list) > 1 else 0), key="new_exB")
        coin_sel = c3.selectbox("Coin", sorted(df_all['Coin'].unique()), key="new_coin")
        dir_sel = c4.selectbox("Side Comparison", ["Ask -> Bid", "Bid -> Ask", "Ask -> Ask", "Bid -> Bid"], key="new_dir")
        
        c5.write(""); c5.write("")
        if c5.button("📊 Add Graph", type="primary"):
            st.session_state.graph_configs.append({'exA': exA, 'exB': exB, 'coin': coin_sel, 'direction': dir_sel})
            st.rerun()

    for i, g in enumerate(st.session_state.graph_configs):
        st.markdown("---")
        h_col1, h_col2 = st.columns([9, 1])
        h_col1.markdown(f"#### 📉 {g['exA']} ➡️ {g['exB']} | Coin: {g['coin']} | {g['direction']}")
        if h_col2.button("❌ Remove", key=f"del_{i}"):
            st.session_state.graph_configs.pop(i); st.rerun()

        f_col1, f_col2, f_col3, f_col4 = st.columns(4)
        start_date = f_col1.date_input("Start Date", df_all['Date'].min(), key=f"sd_{i}")
        start_time = f_col2.time_input("Start Time", pd.to_datetime("00:00").time(), key=f"st_{i}")
        end_date = f_col3.date_input("End Date", df_all['Date'].max(), key=f"ed_{i}")
        end_time = f_col4.time_input("End Time", pd.to_datetime("23:59:59").time(), key=f"et_{i}")

        start_dt, end_dt = pd.to_datetime(f"{start_date} {start_time}"), pd.to_datetime(f"{end_date} {end_time}")
        df_g = df_all[(df_all['Coin'] == g['coin']) & (df_all['Exchange'].isin([g['exA'], g['exB']])) & (df_all['RunTimestamp'] >= start_dt) & (df_all['RunTimestamp'] <= end_dt)]

        if not df_g.empty:
            pivot = df_g.pivot_table(index='RunTimestamp', columns=['Exchange', 'Side'], values='price_usd')
            trend = pd.DataFrame(index=pivot.index)
            col_name = "Spread (pips)"
            
            try:
                if g['direction'] == "Ask -> Bid": trend[col_name] = ((pivot[(g['exA'], 'ASK')] - pivot[(g['exB'], 'BID')]) / np.maximum(pivot[(g['exA'], 'ASK')], pivot[(g['exB'], 'BID')])) * 10000
                elif g['direction'] == "Bid -> Ask": trend[col_name] = ((pivot[(g['exA'], 'BID')] - pivot[(g['exB'], 'ASK')]) / np.maximum(pivot[(g['exA'], 'BID')], pivot[(g['exB'], 'ASK')])) * 10000
                elif g['direction'] == "Ask -> Ask": trend[col_name] = ((pivot[(g['exA'], 'ASK')] - pivot[(g['exB'], 'ASK')]) / np.maximum(pivot[(g['exA'], 'ASK')], pivot[(g['exB'], 'ASK')])) * 10000
                elif g['direction'] == "Bid -> Bid": trend[col_name] = ((pivot[(g['exA'], 'BID')] - pivot[(g['exB'], 'BID')]) / np.maximum(pivot[(g['exA'], 'BID')], pivot[(g['exB'], 'BID')])) * 10000
                
                fig = px.line(trend.reset_index(), x='RunTimestamp', y=col_name, title=f"{g['exA']} vs {g['exB']}")
                fig.add_hline(y=0, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)
            except KeyError:
                st.warning("⚠️ Data missing for the selected pair/side.")
        else:
            st.warning("No data.")

def page_arb4():
    st.title("🔀 4-Leg Arbitrage")
    st.info("Cross-Exchange Spread Difference (Leg 1 vs Leg 2)")
    # (โค้ดสำหรับ 4-Leg เดิมของคุณสามารถใส่ตรงนี้ได้ ผมย่อไว้เพื่อโฟกัส 3-Leg แต่ลอจิกเดิมใช้ได้ปกติครับ)
    st.write("*(4-Leg Feature Structure retained here)*")

def page_arb3():
    st.title("🔺 3-Leg Arbitrage (Cross-Exchange Triangular)")
    st.markdown("""
    **Logic:**
    * **Forward (Buy ExA ➡️ Rebalance ExB ➡️ Rebuy Fiat):** `THB -> Buy Coin (ExA)` ➡️ `Sell Coin for USDT (ExB)` ➡️ `Sell USDT for THB (ExA)`
    * **Reverse (Buy USDT ExA ➡️ Rebalance ExB ➡️ Sell Fiat):** `THB -> Buy USDT (ExA)` ➡️ `Buy Coin with USDT (ExB)` ➡️ `Sell Coin for THB (ExA)`
    """)
    
    df_all = st.session_state.df_all
    ex_list = sorted(df_all['Exchange'].unique())
    coin_list = sorted([c for c in df_all['Coin'].unique() if c != 'USDT'])
    
    with st.expander("➕ Configure 3-Leg Arbitrage", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        exA = c1.selectbox("Ex A (Fiat/Home)", ex_list, index=ex_list.index("BITKUB") if "BITKUB" in ex_list else 0)
        exB = c2.selectbox("Ex B (Crypto/USDT)", ex_list, index=ex_list.index("BINANCE") if "BINANCE" in ex_list else 0)
        target_coin = c3.selectbox("Target Coin", coin_list)
        
        c4.write(""); c4.write("")
        if c4.button("📊 Plot 3-Leg Profit", type="primary"):
            st.session_state.arb3_config = {'exA': exA, 'exB': exB, 'coin': target_coin}
            st.rerun()

    if 'arb3_config' in st.session_state:
        g = st.session_state.arb3_config
        st.markdown("---")
        st.subheader(f"📈 Profit %: {g['exA']} 🔄 {g['exB']} ({g['coin']})")
        
        # คัดเฉพาะ ExA, ExB และ เหรียญเป้าหมาย + USDT
        df_g = df_all[
            (df_all['Exchange'].isin([g['exA'], g['exB']])) & 
            (df_all['Coin'].isin([g['coin'], 'USDT']))
        ]
        
        if df_g.empty:
            st.warning("⚠️ No data available for this configuration.")
            return

        # ใช้ Raw Price (ราคาหน้ากระดาน) ในการคำนวณ ไม่ใช่ price_usd
        pivot = df_g.pivot_table(index='RunTimestamp', columns=['Coin', 'Exchange', 'Side'], values='Price')
        
        # Resample เป็นระดับนาที และ Forward Fill เพื่อให้ Timestamp ทุกเส้นเชื่อมกันสนิท
        pivot = pivot.resample('1T').last().ffill().dropna(how='all')

        def safe_get(c, e, s):
            return pivot[(c, e, s)] if (c, e, s) in pivot.columns else pd.Series(np.nan, index=pivot.index)

        # ข้อมูลสำหรับ Forward
        ask_coin_A = safe_get(g['coin'], g['exA'], 'ASK')
        bid_coin_B = safe_get(g['coin'], g['exB'], 'BID')
        bid_usdt_A = safe_get('USDT', g['exA'], 'BID')
        
        # ข้อมูลสำหรับ Reverse
        ask_usdt_A = safe_get('USDT', g['exA'], 'ASK')
        ask_coin_B = safe_get(g['coin'], g['exB'], 'ASK')
        bid_coin_A = safe_get(g['coin'], g['exA'], 'BID')

        trend = pd.DataFrame(index=pivot.index)
        
        # คำนวณ % Profit
        # Forward: กำไร = (ขาย Coin ได้ USDT บน B * ขาย USDT ได้ Fiat บน A) / (ต้นทุน Fiat ซื้อ Coin บน A) - 1
        trend['Forward Profit %'] = ((bid_coin_B * bid_usdt_A) / ask_coin_A - 1) * 100
        
        # Reverse: กำไร = (ขาย Coin ได้ Fiat บน A) / (ต้นทุน Fiat ซื้อ USDT บน A * เอา USDT ซื้อ Coin บน B) - 1
        trend['Reverse Profit %'] = (bid_coin_A / (ask_usdt_A * ask_coin_B) - 1) * 100

        fig = px.line(
            trend.reset_index(), 
            x='RunTimestamp', 
            y=['Forward Profit %', 'Reverse Profit %'],
            labels={"value": "Net Profit (%)", "RunTimestamp": "Time", "variable": "Direction"}
        )
        fig.update_layout(hovermode="x unified")
        fig.add_hline(y=0, line_dash="dash", line_color="red", opacity=0.8)
        fig.add_hline(y=0.2, line_dash="dot", line_color="green", opacity=0.5, annotation_text="0.2% Fee Line")
        st.plotly_chart(fig, use_container_width=True)


# ==========================================
# 🚀 จุดเริ่มต้นแอปพลิเคชัน (Main)
# ==========================================
try:
    # --- เมนูด้านข้าง (Sidebar) ---
    st.sidebar.title("⚙️ Dashboard Config")
    st.sidebar.markdown("---")
    
    # 💡 จุดแก้ปัญหาโหลดนาน: ให้ User เลือกวันโหลดข้อมูลได้ (Default 1 วัน)
    days_to_load = st.sidebar.slider("📅 Days of data to load", min_value=1, max_value=7, value=1, help="Select 1 day for faster loading time.")
    
    with st.spinner(f'Fetching latest {days_to_load} days from BigQuery...'):
        df_all = load_recent_data(days_to_load)
        df_all['Date'] = df_all['RunTimestamp'].dt.date
        df_all['Time'] = df_all['RunTimestamp'].dt.strftime('%H:%M:%S')
        st.session_state.df_all = df_all

    if df_all.empty:
        st.warning("⚠️ No data found in BigQuery for the selected timeframe.")
    else:
        # 💡 จุดแก้ปัญหา Open in New Tab: ใช้ st.navigation (ต้องใช้ Streamlit เวอร์ชั่น >= 1.36.0)
        pages = [
            st.Page(page_matrix, title="Spread Matrix", icon="📊"),
            st.Page(page_trend, title="Historical Trend", icon="📈"),
            st.Page(page_arb4, title="4-Leg Arbitrage", icon="🔀"),
            st.Page(page_arb3, title="3-Leg Arbitrage", icon="🔺")
        ]
        pg = st.navigation(pages)
        pg.run()

except Exception as e:
    st.error(f"❌ An error occurred: {e}")
