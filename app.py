import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import sqlite3
from datetime import datetime, timedelta

# Set page configuration
st.set_page_config(
    page_title="Vehicle Registration Dashboard",
    page_icon="🚗",
    layout="wide"
)

# Title and introduction
st.title("Vehicle Registration Dashboard")
st.markdown("An interactive dashboard for vehicle registration data from an investor's perspective")

# Initialize session state for data
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

# Function to load data from SQLite database
@st.cache_resource
def load_data_from_sqlite(db_path='vehicle_data.db'):
    """
    Load data from SQLite database
    """
    if not os.path.exists(db_path):
        st.error("Database not found. Please run the data processing script first.")
        return None
    
    conn = sqlite3.connect(db_path)
    
    # Load main data
    df = pd.read_sql('SELECT * FROM vehicle_registrations', conn)
    
    # Convert date columns
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    
    # Load growth metrics
    try:
        monthly_cat = pd.read_sql('SELECT * FROM monthly_category_growth', conn)
        if 'year_month' in monthly_cat.columns:
            monthly_cat['year_month'] = pd.to_datetime(monthly_cat['year_month'])
    except:
        monthly_cat = None
    
    try:
        yearly_cat = pd.read_sql('SELECT * FROM yearly_category_growth', conn)
    except:
        yearly_cat = None
    
    try:
        quarterly_cat = pd.read_sql('SELECT * FROM quarterly_category_growth', conn)
    except:
        quarterly_cat = None
    
    try:
        monthly_man = pd.read_sql('SELECT * FROM monthly_manufacturer_growth', conn)
        if 'year_month' in monthly_man.columns:
            monthly_man['year_month'] = pd.to_datetime(monthly_man['year_month'])
    except:
        monthly_man = None
    
    try:
        yearly_man = pd.read_sql('SELECT * FROM yearly_manufacturer_growth', conn)
    except:
        yearly_man = None
    
    try:
        quarterly_man = pd.read_sql('SELECT * FROM quarterly_manufacturer_growth', conn)
    except:
        quarterly_man = None
    
    conn.close()
    
    return df, monthly_cat, yearly_cat, quarterly_cat, monthly_man, yearly_man, quarterly_man

# Function to load data from CSV files
@st.cache_resource
def load_data_from_csv(processed_dir='processed_data'):
    """
    Load data from CSV files
    """
    # Find the most recent cleaned data file
    cleaned_files = [f for f in os.listdir(processed_dir) if f.startswith('cleaned_data_')]
    if not cleaned_files:
        st.error("No processed data found. Please run the data processing script first.")
        return None, None, None, None, None, None, None
    
    latest_cleaned = max(cleaned_files, key=lambda x: os.path.getmtime(os.path.join(processed_dir, x)))
    df = pd.read_csv(os.path.join(processed_dir, latest_cleaned))
    
    # Convert date column to datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    
    # Find growth metrics files
    monthly_cat_files = [f for f in os.listdir(processed_dir) if f.startswith('monthly_category_growth_')]
    yearly_cat_files = [f for f in os.listdir(processed_dir) if f.startswith('yearly_category_growth_')]
    quarterly_cat_files = [f for f in os.listdir(processed_dir) if f.startswith('quarterly_category_growth_')]
    monthly_man_files = [f for f in os.listdir(processed_dir) if f.startswith('monthly_manufacturer_growth_')]
    yearly_man_files = [f for f in os.listdir(processed_dir) if f.startswith('yearly_manufacturer_growth_')]
    quarterly_man_files = [f for f in os.listdir(processed_dir) if f.startswith('quarterly_manufacturer_growth_')]
    
    # Load the most recent files
    if monthly_cat_files:
        latest_monthly_cat = max(monthly_cat_files, key=lambda x: os.path.getmtime(os.path.join(processed_dir, x)))
        monthly_cat = pd.read_csv(os.path.join(processed_dir, latest_monthly_cat))
        if 'year_month' in monthly_cat.columns:
            monthly_cat['year_month'] = pd.to_datetime(monthly_cat['year_month'])
    else:
        monthly_cat = None
    
    if yearly_cat_files:
        latest_yearly_cat = max(yearly_cat_files, key=lambda x: os.path.getmtime(os.path.join(processed_dir, x)))
        yearly_cat = pd.read_csv(os.path.join(processed_dir, latest_yearly_cat))
    else:
        yearly_cat = None
    
    if quarterly_cat_files:
        latest_quarterly_cat = max(quarterly_cat_files, key=lambda x: os.path.getmtime(os.path.join(processed_dir, x)))
        quarterly_cat = pd.read_csv(os.path.join(processed_dir, latest_quarterly_cat))
    else:
        quarterly_cat = None
    
    if monthly_man_files:
        latest_monthly_man = max(monthly_man_files, key=lambda x: os.path.getmtime(os.path.join(processed_dir, x)))
        monthly_man = pd.read_csv(os.path.join(processed_dir, latest_monthly_man))
        if 'year_month' in monthly_man.columns:
            monthly_man['year_month'] = pd.to_datetime(monthly_man['year_month'])
    else:
        monthly_man = None
    
    if yearly_man_files:
        latest_yearly_man = max(yearly_man_files, key=lambda x: os.path.getmtime(os.path.join(processed_dir, x)))
        yearly_man = pd.read_csv(os.path.join(processed_dir, latest_yearly_man))
    else:
        yearly_man = None
    
    if quarterly_man_files:
        latest_quarterly_man = max(quarterly_man_files, key=lambda x: os.path.getmtime(os.path.join(processed_dir, x)))
        quarterly_man = pd.read_csv(os.path.join(processed_dir, latest_quarterly_man))
    else:
        quarterly_man = None
    
    return df, monthly_cat, yearly_cat, quarterly_cat, monthly_man, yearly_man, quarterly_man

# Load data
df, monthly_cat, yearly_cat, quarterly_cat, monthly_man, yearly_man, quarterly_man = load_data_from_sqlite()

if df is None:
    # Try to load from CSV files
    df, monthly_cat, yearly_cat, quarterly_cat, monthly_man, yearly_man, quarterly_man = load_data_from_csv()

if df is not None:
    st.session_state.data_loaded = True
    
    # Sidebar for filters
    st.sidebar.header("Filters")
    
    # Date range filter
    if 'date' in df.columns:
        min_date = df['date'].min().date()
        max_date = df['date'].max().date()
        
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        # Filter data based on date range
        start_date, end_date = date_range
        mask = (df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)
        df_filtered = df.loc[mask].copy()
    else:
        df_filtered = df.copy()
    
    # Vehicle category filter
    if 'category' in df_filtered.columns:
        categories = sorted(df_filtered['category'].unique())
        selected_categories = st.sidebar.multiselect(
            "Select Vehicle Categories",
            options=categories,
            default=categories
        )
        
        # Filter data based on selected categories
        df_filtered = df_filtered[df_filtered['category'].isin(selected_categories)]
    
    # Manufacturer filter
    if 'manufacturer' in df_filtered.columns:
        manufacturers = sorted(df_filtered['manufacturer'].unique())
        selected_manufacturers = st.sidebar.multiselect(
            "Select Manufacturers",
            options=manufacturers,
            default=manufacturers[:5] if len(manufacturers) > 5 else manufacturers
        )
        
        # Filter data based on selected manufacturers
        df_filtered = df_filtered[df_filtered['manufacturer'].isin(selected_manufacturers)]
    
    # Display key metrics
    st.header("Key Metrics")
    
    # Calculate total registrations
    if 'registrations' in df_filtered.columns:
        total_registrations = df_filtered['registrations'].sum()
        st.metric("Total Registrations", f"{total_registrations:,.0f}")
    
    # Display YoY and QoQ growth metrics
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Year-over-Year Growth")
        
        if yearly_cat is not None:
            # Filter YoY data based on selected categories
            if 'category' in yearly_cat.columns and selected_categories:
                yoy_cat_filtered = yearly_cat[yearly_cat['category'].isin(selected_categories)]
                
                # Get the latest year
                latest_year = yoy_cat_filtered['year'].max()
                yoy_latest = yoy_cat_filtered[yoy_cat_filtered['year'] == latest_year]
                
                # Create a bar chart
                fig_yoy_cat = px.bar(
                    yoy_latest,
                    x='category',
                    y='yoy_growth',
                    title=f"YoY Growth by Category ({latest_year})",
                    labels={'yoy_growth': 'YoY Growth (%)'},
                    color='category'
                )
                
                st.plotly_chart(fig_yoy_cat, use_container_width=True)
        
        if yearly_man is not None:
            # Filter YoY data based on selected manufacturers
            if 'manufacturer' in yearly_man.columns and selected_manufacturers:
                yoy_man_filtered = yearly_man[yearly_man['manufacturer'].isin(selected_manufacturers)]
                
                # Get the latest year
                latest_year = yoy_man_filtered['year'].max()
                yoy_latest = yoy_man_filtered[yoy_man_filtered['year'] == latest_year]
                
                # Create a bar chart
                fig_yoy_man = px.bar(
                    yoy_latest,
                    x='manufacturer',
                    y='yoy_growth',
                    title=f"YoY Growth by Manufacturer ({latest_year})",
                    labels={'yoy_growth': 'YoY Growth (%)'},
                    color='manufacturer'
                )
                
                st.plotly_chart(fig_yoy_man, use_container_width=True)
    
    with col2:
        st.subheader("Quarter-over-Quarter Growth")
        
        if quarterly_cat is not None:
            # Filter QoQ data based on selected categories
            if 'category' in quarterly_cat.columns and selected_categories:
                qoq_cat_filtered = quarterly_cat[quarterly_cat['category'].isin(selected_categories)]
                
                # Get the latest year and quarter
                latest_year = qoq_cat_filtered['year'].max()
                latest_quarter = qoq_cat_filtered[qoq_cat_filtered['year'] == latest_year]['quarter'].max()
                qoq_latest = qoq_cat_filtered[(qoq_cat_filtered['year'] == latest_year) & 
                                             (qoq_cat_filtered['quarter'] == latest_quarter)]
                
                # Create a bar chart
                fig_qoq_cat = px.bar(
                    qoq_latest,
                    x='category',
                    y='qoq_growth',
                    title=f"QoQ Growth by Category (Q{latest_quarter} {latest_year})",
                    labels={'qoq_growth': 'QoQ Growth (%)'},
                    color='category'
                )
                
                st.plotly_chart(fig_qoq_cat, use_container_width=True)
        
        if quarterly_man is not None:
            # Filter QoQ data based on selected manufacturers
            if 'manufacturer' in quarterly_man.columns and selected_manufacturers:
                qoq_man_filtered = quarterly_man[quarterly_man['manufacturer'].isin(selected_manufacturers)]
                
                # Get the latest year and quarter
                latest_year = qoq_man_filtered['year'].max()
                latest_quarter = qoq_man_filtered[qoq_man_filtered['year'] == latest_year]['quarter'].max()
                qoq_latest = qoq_man_filtered[(qoq_man_filtered['year'] == latest_year) & 
                                             (qoq_man_filtered['quarter'] == latest_quarter)]
                
                # Create a bar chart
                fig_qoq_man = px.bar(
                    qoq_latest,
                    x='manufacturer',
                    y='qoq_growth',
                    title=f"QoQ Growth by Manufacturer (Q{latest_quarter} {latest_year})",
                    labels={'qoq_growth': 'QoQ Growth (%)'},
                    color='manufacturer'
                )
                
                st.plotly_chart(fig_qoq_man, use_container_width=True)
    
    # Display trends over time
    st.header("Trends Over Time")
    
    if 'date' in df_filtered.columns and 'registrations' in df_filtered.columns:
        # Group by date and category
        if 'category' in df_filtered.columns:
            df_category_trend = df_filtered.groupby(['date', 'category'])['registrations'].sum().reset_index()
            
            # Create a line chart for category trends
            fig_category_trend = px.line(
                df_category_trend,
                x='date',
                y='registrations',
                color='category',
                title="Vehicle Registration Trends by Category",
                labels={'registrations': 'Number of Registrations', 'date': 'Date'}
            )
            
            st.plotly_chart(fig_category_trend, use_container_width=True)
        
        # Group by date and manufacturer
        if 'manufacturer' in df_filtered.columns:
            df_manufacturer_trend = df_filtered.groupby(['date', 'manufacturer'])['registrations'].sum().reset_index()
            
            # Create a line chart for manufacturer trends
            fig_manufacturer_trend = px.line(
                df_manufacturer_trend,
                x='date',
                y='registrations',
                color='manufacturer',
                title="Vehicle Registration Trends by Manufacturer",
                labels={'registrations': 'Number of Registrations', 'date': 'Date'}
            )
            
            st.plotly_chart(fig_manufacturer_trend, use_container_width=True)
    
    # Display market share
    st.header("Market Share")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if 'category' in df_filtered.columns and 'registrations' in df_filtered.columns:
            # Calculate market share by category
            category_share = df_filtered.groupby('category')['registrations'].sum().reset_index()
            category_share['share'] = (category_share['registrations'] / category_share['registrations'].sum()) * 100
            
            # Create a pie chart
            fig_category_share = px.pie(
                category_share,
                values='share',
                names='category',
                title="Market Share by Category",
                hover_data=['registrations'],
                labels={'share': 'Market Share (%)'}
            )
            
            st.plotly_chart(fig_category_share, use_container_width=True)
    
    with col2:
        if 'manufacturer' in df_filtered.columns and 'registrations' in df_filtered.columns:
            # Calculate market share by manufacturer
            manufacturer_share = df_filtered.groupby('manufacturer')['registrations'].sum().reset_index()
            manufacturer_share['share'] = (manufacturer_share['registrations'] / manufacturer_share['registrations'].sum()) * 100
            
            # Sort by share and take top 10
            manufacturer_share = manufacturer_share.sort_values('share', ascending=False).head(10)
            
            # Create a pie chart
            fig_manufacturer_share = px.pie(
                manufacturer_share,
                values='share',
                names='manufacturer',
                title="Market Share by Manufacturer (Top 10)",
                hover_data=['registrations'],
                labels={'share': 'Market Share (%)'}
            )
            
            st.plotly_chart(fig_manufacturer_share, use_container_width=True)
    
    # Display data table
    st.header("Data Table")
    
    if st.checkbox("Show raw data"):
        st.subheader("Raw Data")
        st.write(df_filtered)
    
    # Add a section for investor insights
    st.header("Investor Insights")
    
    st.markdown("""
    ### Key Observations:
    
    1. **Market Trends**: The dashboard shows how vehicle registrations have evolved over time, which can indicate economic health and consumer sentiment.
    
    2. **Category Performance**: Different vehicle categories (2W, 3W, 4W) may show varying growth patterns, reflecting changes in consumer preferences and economic factors.
    
    3. **Manufacturer Dynamics**: Tracking manufacturer performance can reveal competitive dynamics and market share shifts.
    
    4. **Seasonal Patterns**: Quarterly data may reveal seasonal patterns in vehicle purchases, which can be important for forecasting.
    
    ### Potential Investment Insights:
    
    - Manufacturers with consistent YoY growth may represent stable investment opportunities.
    - Emerging trends in vehicle categories (e.g., electric vehicles) might indicate future growth areas.
    - Market share gains by specific manufacturers could signal competitive advantages.
    """)

else:
    st.error("No data available. Please run the data processing script first.")
    st.info("""
    To get started:
    1. Run the data scraping script: `python scrape_vahan_data.py`
    2. Process the data: `python process_data.py`
    3. Refresh this page
    """)

# Add a footer
st.sidebar.markdown("---")
st.sidebar.markdown("Built with Streamlit")