import pandas as pd
import numpy as np
import os
from datetime import datetime
import sqlite3

def load_data(data_dir='data'):
    """
    Load all CSV files from the data directory and combine them into a single DataFrame
    """
    all_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    
    if not all_files:
        print("No data files found.")
        return None
    
    dfs = []
    for filename in all_files:
        filepath = os.path.join(data_dir, filename)
        try:
            df = pd.read_csv(filepath)
            dfs.append(df)
            print(f"Loaded {filename}")
        except Exception as e:
            print(f"Error loading {filename}: {e}")
    
    if not dfs:
        return None
    
    # Combine all DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)
    
    return combined_df

def clean_data(df):
    """
    Clean the data by handling missing values, converting data types, etc.
    """
    # Make a copy of the DataFrame to avoid SettingWithCopyWarning
    df_clean = df.copy()
    
    # Drop rows with all NA values
    df_clean.dropna(how='all', inplace=True)
    
    # Standardize column names
    df_clean.columns = [col.strip().lower().replace(' ', '_') for col in df_clean.columns]
    
    # Convert date columns to datetime format
    date_cols = [col for col in df_clean.columns if 'date' in col]
    for col in date_cols:
        try:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        except:
            pass
    
    # Convert numeric columns
    numeric_cols = [col for col in df_clean.columns if any(word in col.lower() for word in 
                   ['registration', 'value', 'number', 'count', 'sales'])]
    
    for col in numeric_cols:
        try:
            # Remove any non-numeric characters
            df_clean[col] = df_clean[col].astype(str).str.replace(r'[^\d.]', '', regex=True)
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        except:
            pass
    
    # Standardize category names
    if 'category' in df_clean.columns:
        df_clean['category'] = df_clean['category'].str.upper()
        df_clean['category'] = df_clean['category'].replace({
            'TWO WHEELER': '2W',
            'THREE WHEELER': '3W',
            'FOUR WHEELER': '4W',
            '2 WHEELER': '2W',
            '3 WHEELER': '3W',
            '4 WHEELER': '4W'
        })
    
    # Standardize manufacturer names
    if 'manufacturer' in df_clean.columns:
        df_clean['manufacturer'] = df_clean['manufacturer'].str.title()
        df_clean['manufacturer'] = df_clean['manufacturer'].replace({
            'Hero': 'Hero MotoCorp',
            'Bajaj': 'Bajaj Auto',
            'Tata': 'Tata Motors',
            'Mahindra': 'Mahindra & Mahindra',
            'Mahindra And Mahindra': 'Mahindra & Mahindra'
        })
    
    return df_clean

def calculate_growth_metrics(df, date_col='date', value_col='registrations', group_col='category'):
    """
    Calculate YoY and QoQ growth metrics
    """
    # Make a copy of the DataFrame to avoid SettingWithCopyWarning
    df_result = df.copy()
    
    # Extract year and quarter from the date column
    df_result['year'] = df_result[date_col].dt.year
    df_result['quarter'] = df_result[date_col].dt.quarter
    df_result['month'] = df_result[date_col].dt.month
    df_result['year_month'] = df_result[date_col].dt.to_period('M')
    
    # Group by year and month and the specified group column
    monthly = df_result.groupby(['year_month', group_col])[value_col].sum().reset_index()
    monthly['year_month'] = monthly['year_month'].dt.to_timestamp()
    
    # Calculate month-over-month growth
    monthly['mom_growth'] = monthly.groupby(group_col)[value_col].pct_change() * 100
    
    # Group by year and the specified group column
    yearly = df_result.groupby(['year', group_col])[value_col].sum().reset_index()
    
    # Calculate YoY growth
    yearly['yoy_growth'] = yearly.groupby(group_col)[value_col].pct_change() * 100
    
    # Group by year, quarter, and the specified group column
    quarterly = df_result.groupby(['year', 'quarter', group_col])[value_col].sum().reset_index()
    
    # Calculate QoQ growth
    quarterly['qoq_growth'] = quarterly.groupby([group_col, 'year'])[value_col].pct_change() * 100
    
    return monthly, yearly, quarterly

def save_to_sqlite(df, db_path='vehicle_data.db'):
    """
    Save DataFrame to SQLite database
    """
    conn = sqlite3.connect(db_path)
    
    # Save the main data
    df.to_sql('vehicle_registrations', conn, if_exists='replace', index=False)
    
    # Calculate and save growth metrics
    if 'category' in df.columns:
        monthly_cat, yearly_cat, quarterly_cat = calculate_growth_metrics(df, group_col='category')
        
        monthly_cat.to_sql('monthly_category_growth', conn, if_exists='replace', index=False)
        yearly_cat.to_sql('yearly_category_growth', conn, if_exists='replace', index=False)
        quarterly_cat.to_sql('quarterly_category_growth', conn, if_exists='replace', index=False)
    
    if 'manufacturer' in df.columns:
        monthly_man, yearly_man, quarterly_man = calculate_growth_metrics(df, group_col='manufacturer')
        
        monthly_man.to_sql('monthly_manufacturer_growth', conn, if_exists='replace', index=False)
        yearly_man.to_sql('yearly_manufacturer_growth', conn, if_exists='replace', index=False)
        quarterly_man.to_sql('quarterly_manufacturer_growth', conn, if_exists='replace', index=False)
    
    conn.close()
    print(f"Data saved to SQLite database: {db_path}")

def process_data(data_dir='data', output_dir='processed_data'):
    """
    Main function to process the data
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Load the data
    df = load_data(data_dir)
    if df is None:
        print("No data to process.")
        return None
    
    # Clean the data
    df_clean = clean_data(df)
    
    # Save the cleaned data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df_clean.to_csv(f'{output_dir}/cleaned_data_{timestamp}.csv', index=False)
    print(f"Saved cleaned data to {output_dir}/cleaned_data_{timestamp}.csv")
    
    # Save to SQLite database
    save_to_sqlite(df_clean)
    
    # Calculate growth metrics for vehicle categories
    if 'category' in df_clean.columns:
        monthly_cat, yearly_cat, quarterly_cat = calculate_growth_metrics(df_clean, group_col='category')
        
        # Save the results
        monthly_cat.to_csv(f'{output_dir}/monthly_category_growth_{timestamp}.csv', index=False)
        yearly_cat.to_csv(f'{output_dir}/yearly_category_growth_{timestamp}.csv', index=False)
        quarterly_cat.to_csv(f'{output_dir}/quarterly_category_growth_{timestamp}.csv', index=False)
        
        print(f"Saved category growth metrics to {output_dir}/")
    
    # Calculate growth metrics for manufacturers
    if 'manufacturer' in df_clean.columns:
        monthly_man, yearly_man, quarterly_man = calculate_growth_metrics(df_clean, group_col='manufacturer')
        
        # Save the results
        monthly_man.to_csv(f'{output_dir}/monthly_manufacturer_growth_{timestamp}.csv', index=False)
        yearly_man.to_csv(f'{output_dir}/yearly_manufacturer_growth_{timestamp}.csv', index=False)
        quarterly_man.to_csv(f'{output_dir}/quarterly_manufacturer_growth_{timestamp}.csv', index=False)
        
        print(f"Saved manufacturer growth metrics to {output_dir}/")
    
    return df_clean, monthly_cat, yearly_cat, quarterly_cat, monthly_man, yearly_man, quarterly_man

if __name__ == "__main__":
    process_data()