import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime
import json

def scrape_vahan_dashboard():
    """
    Function to scrape data from Vahan Dashboard
    Note: Since Vahan Dashboard may require authentication or have dynamic content,
    this is a template that might need adjustments based on actual website structure.
    """
    # Create directories if they don't exist
    if not os.path.exists('data'):
        os.makedirs('data')
    
    # URL for Vahan Dashboard - this may need to be updated
    base_url = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
    
    # Headers to mimic browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # Create a session to maintain cookies
        session = requests.Session()
        response = session.get(base_url, headers=headers)
        response.raise_for_status()
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # If the site uses JavaScript to load data, we might need to look for API endpoints
        # For now, let's try to find data in script tags or tables
        scripts = soup.find_all('script')
        data_found = False
        
        # Look for JSON data in script tags
        for script in scripts:
            if 'vehicleData' in script.text or 'chartData' in script.text:
                try:
                    # Extract JSON data (this is a generic approach)
                    json_text = script.text
                    start_idx = json_text.find('{')
                    end_idx = json_text.rfind('}') + 1
                    
                    if start_idx != -1 and end_idx != -1:
                        json_data = json.loads(json_text[start_idx:end_idx])
                        
                        # Convert to DataFrame
                        if isinstance(json_data, dict):
                            df = pd.json_normalize(json_data)
                        elif isinstance(json_data, list):
                            df = pd.DataFrame(json_data)
                        else:
                            continue
                        
                        # Save the data
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        df.to_csv(f'data/vahan_data_{timestamp}.csv', index=False)
                        print(f"Saved data to data/vahan_data_{timestamp}.csv")
                        data_found = True
                        
                except Exception as e:
                    print(f"Error parsing JSON data: {e}")
        
        # If no JSON data found, try to extract from tables
        if not data_found:
            tables = soup.find_all('table')
            
            for i, table in enumerate(tables):
                # Extract table headers
                headers = []
                for th in table.find_all('th'):
                    headers.append(th.text.strip())
                
                # Extract table rows
                rows = []
                for tr in table.find_all('tr')[1:]:  # Skip the header row
                    cells = tr.find_all('td')
                    row = [cell.text.strip() for cell in cells]
                    rows.append(row)
                
                if rows:
                    # Create a DataFrame
                    df = pd.DataFrame(rows, columns=headers)
                    
                    # Save the DataFrame to a CSV file
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    df.to_csv(f'data/vahan_table_{i}_{timestamp}.csv', index=False)
                    print(f"Saved table {i} to data/vahan_table_{i}_{timestamp}.csv")
                    data_found = True
        
        if not data_found:
            print("No data could be extracted. The website might use JavaScript to load data dynamically.")
            print("Consider using Selenium or checking for API endpoints.")
        
        return data_found
    
    except Exception as e:
        print(f"Error scraping data: {e}")
        return False

# Alternative approach using API endpoints if available
def get_vahan_api_data():
    """
    Attempt to get data from Vahan API endpoints if available
    This is a template function and will need to be adapted based on actual API structure
    """
    # Create directories if they don't exist
    if not os.path.exists('data'):
        os.makedirs('data')
    
    # Example API endpoints (these are hypothetical and need to be replaced with actual endpoints)
    api_endpoints = [
        "https://vahan.parivahan.gov.in/vahan4dashboard/api/vehicleCategory",
        "https://vahan.parivahan.gov.in/vahan4dashboard/api/manufacturerData"
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    for endpoint in api_endpoints:
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            
            # Convert to DataFrame
            if isinstance(data, dict):
                df = pd.json_normalize(data)
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                continue
            
            # Save the data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            endpoint_name = endpoint.split('/')[-1]
            df.to_csv(f'data/vahan_{endpoint_name}_{timestamp}.csv', index=False)
            print(f"Saved API data to data/vahan_{endpoint_name}_{timestamp}.csv")
            
        except Exception as e:
            print(f"Error getting data from {endpoint}: {e}")

def generate_sample_data():
    """
    Generate sample data for testing purposes if scraping doesn't work
    """
    # Create directories if they don't exist
    if not os.path.exists('data'):
        os.makedirs('data')
    
    # Define date range
    start_date = '2021-01-01'
    end_date = '2023-12-31'
    dates = pd.date_range(start=start_date, end=end_date, freq='M')
    
    # Define vehicle categories
    categories = ['2W', '3W', '4W']
    
    # Define manufacturers
    manufacturers = [
        'Hero MotoCorp', 'Honda', 'TVS', 'Bajaj Auto', 'Royal Enfield',
        'Maruti Suzuki', 'Hyundai', 'Tata Motors', 'Mahindra', 'Toyota',
        'Kia', 'MG Motor', 'Renault', 'Nissan', 'Volkswagen'
    ]
    
    # Generate sample data
    data = []
    for date in dates:
        for category in categories:
            # Base number varies by category and has some seasonality
            if category == '2W':
                base = 500000 + 100000 * (date.month % 12) / 12
            elif category == '3W':
                base = 50000 + 10000 * (date.month % 12) / 12
            else:  # 4W
                base = 300000 + 50000 * (date.month % 12) / 12
            
            # Add some random variation
            registrations = int(base * (0.8 + 0.4 * np.random.random()))
            
            # Add a row for the category total
            data.append({
                'Date': date,
                'Category': category,
                'Manufacturer': 'All',
                'Registrations': registrations
            })
            
            # Distribute registrations among manufacturers
            if category == '2W':
                relevant_manufacturers = manufacturers[:5]
            elif category == '3W':
                relevant_manufacturers = manufacturers[5:7]
            else:  # 4W
                relevant_manufacturers = manufacturers[5:]
            
            # Generate manufacturer-specific data
            man_shares = np.random.dirichlet(np.ones(len(relevant_manufacturers)), size=1)[0]
            
            for i, manufacturer in enumerate(relevant_manufacturers):
                man_registrations = int(registrations * man_shares[i])
                data.append({
                    'Date': date,
                    'Category': category,
                    'Manufacturer': manufacturer,
                    'Registrations': man_registrations
                })
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Save the data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df.to_csv(f'data/vahan_sample_data_{timestamp}.csv', index=False)
    print(f"Generated sample data: data/vahan_sample_data_{timestamp}.csv")
    
    return df

if __name__ == "__main__":
    # Try to scrape the website
    print("Attempting to scrape Vahan Dashboard...")
    success = scrape_vahan_dashboard()
    
    # If scraping fails, try API endpoints
    if not success:
        print("\nAttempting to get data from API endpoints...")
        get_vahan_api_data()
    
    # If both methods fail, generate sample data for testing
    if not success and not any(f.startswith('vahan_') for f in os.listdir('data')):
        print("\nGenerating sample data for testing...")
        generate_sample_data()