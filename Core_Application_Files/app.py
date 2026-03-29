import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import db_utils

# --- Page Configuration ---
st.set_page_config(
    page_title="PhonePe Pulse - Transaction Insights",
    page_icon="💸",
    layout="wide",
)

# --- Theme Styling ---
st.markdown("""
<style>
    .stApp { background-color: #f8f9fa; }
    .main h1 { color: #6739B7; font-weight: 800; }
    .stSidebar { background-color: #6739B7; color: white; }
    .stMetric { background-color: white; padding: 10px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
</style>
""", unsafe_allow_html=True)

# --- Data Loading (Cached) ---
@st.cache_data
def load_geojson():
    with open("india_states.json", "r") as f:
        return json.load(f)

geojson = load_geojson()

# --- Sidebar ---
st.sidebar.image("https://www.logo.wine/a/logo/PhonePe/PhonePe-Logo.wine.svg", width=200)
st.sidebar.title("PhonePe Pulse Analysis")
menu = st.sidebar.radio("Navigate", 
    ["🏠 Home", "🗺️ Geographical Analysis", "📈 Growth Trends", "📱 Device Insights", "🏆 Top Performers"])

years = db_utils.get_years()
selected_year = st.sidebar.selectbox("Select Year", years, index=len(years)-1)
selected_quarter = st.sidebar.selectbox("Select Quarter", [1, 2, 3, 4], index=0)

# --- Home Page ---
if menu == "🏠 Home":
    st.title("💸 PhonePe Pulse: Transaction Insights")
    st.markdown("### Powering Digital India with Comprehensive Financial Data (2018-2024)")
    
    # KPIs
    df_home = db_utils.get_aggregated_transaction(selected_year, selected_quarter)
    total_count = df_home['transaction_count'].sum()
    total_amount = df_home['transaction_amount'].sum()
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Transactions", f"{total_count:,}")
    col2.metric("Total Value", f"₹{total_amount/1e7:,.2f} Cr")
    
    df_user = db_utils.get_aggregated_user(selected_year, selected_quarter)
    total_users = df_user['registered_users'].max()  # Country-level approx
    col3.metric("Registered Users", f"{total_users:,}")

    st.image("https://www.phonepe.com/content/uploads/2023/04/Banner_Image-scaled.jpg", use_container_width=True)
    st.info("Select a menu from the sidebar to dive deeper into geographical, temporal, or device-level insights.")

# --- Geographical Analysis ---
elif menu == "🗺️ Geographical Analysis":
    st.title("🗺️ Geographical Transaction Dynamics")
    st.markdown("Exploring digital payment adoption across Indian states and districts.")

    df_map = db_utils.get_aggregated_transaction(selected_year, selected_quarter)
    df_map_grouped = df_map.groupby('state')['transaction_amount'].sum().reset_index()

    # Choropleth Map
    fig = px.choropleth(
        df_map_grouped,
        geojson=geojson,
        featureidkey="properties.ST_NM",
        locations="state",
        color="transaction_amount",
        color_continuous_scale="Viridis",
        title=f"Transaction Value by State (Year {selected_year}, Quarter {selected_quarter})",
        labels={'transaction_amount': 'Value (INR)'}
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin={"r":0,"t":50,"l":0,"b":0}, height=600)
    st.plotly_chart(fig, use_container_width=True)

    # District Level
    st.subheader("🏙️ Top Districts in State")
    selected_state = st.selectbox("Select a State to see District performance", sorted(df_map['state'].unique()))
    df_dist = db_utils.get_map_transaction(selected_year, selected_quarter)
    df_dist_state = df_dist[df_dist['state'] == selected_state].groupby('district')['transaction_amount'].sum().sort_values(ascending=False).head(10).reset_index()
    
    fig_dist = px.bar(df_dist_state, x='transaction_amount', y='district', orientation='h', 
                    title=f"Top Districts in {selected_state}", color='transaction_amount')
    st.plotly_chart(fig_dist, use_container_width=True)

# --- Growth Trends ---
elif menu == "📈 Growth Trends":
    st.title("📈 Growth & Performance Trends")
    
    # Transaction growth
    df_agg = db_utils.get_aggregated_transaction()
    df_trend = df_agg.groupby(['year', 'quarter'])['transaction_amount'].sum().reset_index()
    df_trend['Period'] = df_trend['year'].astype(str) + " Q" + df_trend['quarter'].astype(str)
    
    fig_line = px.line(df_trend, x='Period', y='transaction_amount', markers=True, 
                     title="Overall Transaction Value growth over time", color_discrete_sequence=['#6739B7'])
    st.plotly_chart(fig_line, use_container_width=True)

    # Insurance vs Transactions
    st.subheader("🛡️ Insurance Adoption Segment")
    df_ins = db_utils.get_insurance_data()
    df_ins_trend = df_ins.groupby(['year', 'quarter'])['transaction_count'].sum().reset_index()
    df_ins_trend['Period'] = df_ins_trend['year'].astype(str) + " Q" + df_ins_trend['quarter'].astype(str)
    fig_ins = px.bar(df_ins_trend, x='Period', y='transaction_count', title="Insurance Policy Purchases Volume")
    st.plotly_chart(fig_ins, use_container_width=True)

# --- Device Insights ---
elif menu == "📱 Device Insights":
    st.title("📱 User Engagement & Device Ecosystem")
    
    df_user = db_utils.get_aggregated_user(selected_year, selected_quarter)
    df_brands = df_user[df_user['brand'].notna()].groupby('brand')['brand_count'].sum().reset_index()
    
    fig_pie = px.pie(df_brands, values='brand_count', names='brand', title="Market Share of Smartphone Brands",
                    hole=.4, color_discrete_sequence=px.colors.sequential.RdBu)
    st.plotly_chart(fig_pie, use_container_width=True)
    
    st.markdown("#### App Engagement Metric")
    st.write("Average App Opens per state (engagement intensity):")
    df_opens = df_user.groupby('state')['app_opens'].mean().sort_values(ascending=False).head(10).reset_index()
    fig_opens = px.bar(df_opens, x='app_opens', y='state', orientation='h', color='app_opens')
    st.plotly_chart(fig_opens, use_container_width=True)

# --- Top Performers ---
elif menu == "🏆 Top Performers":
    st.title("🏆 Leaderboard of Digital Adoption")
    
    tab1, tab2, tab3 = st.tabs(["States", "Districts", "Pincodes"])
    
    with tab1:
        df_top_s = db_utils.get_top_data('state')
        df_top_s_grouped = df_top_s.groupby('entity_name')['transaction_amount'].sum().sort_values(ascending=False).head(10).reset_index()
        fig_s = px.bar(df_top_s_grouped, x='transaction_amount', y='entity_name', orientation='h', title="Top 10 States by Transaction Value")
        st.plotly_chart(fig_s, use_container_width=True)
        
    with tab2:
        df_top_d = db_utils.get_top_data('district')
        df_top_d_grouped = df_top_d.groupby('entity_name')['transaction_amount'].sum().sort_values(ascending=False).head(10).reset_index()
        fig_d = px.bar(df_top_d_grouped, x='transaction_amount', y='entity_name', orientation='h', title="Top 10 Districts by Transaction Value")
        st.plotly_chart(fig_d, use_container_width=True)
        
    with tab3:
        df_top_p = db_utils.get_top_data('pincode')
        df_top_p_grouped = df_top_p.groupby('entity_name')['transaction_amount'].sum().sort_values(ascending=False).head(10).reset_index()
        fig_p = px.bar(df_top_p_grouped, x='transaction_amount', y='entity_name', orientation='h', title="Top 10 Pincodes by Transaction Value")
        st.plotly_chart(fig_p, use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.markdown("Built with ❤️ using Streamlit & Plotly")
