import requests
import json
from datetime import datetime, timedelta
from database import init_database, insert_crime_records, log_fetch
import time
import pytz

# CST timezone
CST = pytz.timezone('America/Chicago')

API_BASE_URL = "https://data.sanantonio.gov/api/3/action/datastore_search"
RESOURCE_ID = "f36bb931-8fb4-481c-83d9-a3589108bb20"
RECORDS_PER_PAGE = 1000

def fetch_crime_data_page(offset=0, date_filter=None):
    params = {
        'resource_id': RESOURCE_ID,
        'limit': RECORDS_PER_PAGE,
        'offset': offset,
        'sort': 'Report_Date desc'
    }
    
    # The API doesn't seem to support complex filters, so we'll filter after fetching
    # Just get the most recent records and filter in Python
    
    try:
        response = requests.get(API_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def fetch_all_crime_data(days=30):
    print(f"Starting to fetch crime data for the last {days} days...")
    
    # Get the most recent data available (the API seems to have data up to June 30, 2025)
    # We'll fetch the most recent 30 days of available data
    all_records = []
    offset = 0
    total_records = None
    records_needed = days * 400  # Estimate ~400 crimes per day
    
    while len(all_records) < records_needed:
        print(f"Fetching records from offset {offset}...")
        data = fetch_crime_data_page(offset)
        
        if not data or not data.get('success'):
            print("Failed to fetch data")
            break
        
        result = data.get('result', {})
        records = result.get('records', [])
        
        if not records:
            break
        
        # Just add all records - we'll get the most recent 30 days worth
        all_records.extend(records)
        
        # Check if we have enough days of data
        if all_records:
            dates = sorted(set(r['Report_Date'] for r in all_records if 'Report_Date' in r), reverse=True)
            if len(dates) >= days:
                print(f"Fetched {days} days of data, stopping")
                # Filter to exactly the most recent 30 days
                cutoff_date = dates[days-1] if len(dates) >= days else dates[-1]
                all_records = [r for r in all_records if r.get('Report_Date', '') >= cutoff_date]
                break
        
        # Get total from first request
        if total_records is None:
            total_records = result.get('total', 0)
            print(f"Total records available: {total_records}")
        
        # Check if we've fetched all records
        if len(records) < RECORDS_PER_PAGE:
            break
        
        offset += RECORDS_PER_PAGE
        
        # Be nice to the API
        time.sleep(0.5)
    
    print(f"Fetched {len(all_records)} records total")
    
    # Get date range
    if all_records:
        dates = [r['Report_Date'] for r in all_records if 'Report_Date' in r]
        start_date = min(dates) if dates else date_filter
        end_date = max(dates) if dates else datetime.now().strftime('%Y-%m-%d')
    else:
        start_date = end_date = datetime.now().strftime('%Y-%m-%d')
    
    return all_records, start_date, end_date

def refresh_crime_data(days=30):
    current_time = datetime.now(CST).strftime('%B %d, %Y at %I:%M %p CST')
    print(f"Refreshing crime data - {current_time}")
    
    # Initialize database if needed
    init_database()
    
    # Fetch data
    records, start_date, end_date = fetch_all_crime_data(days)
    
    if records:
        # Insert into database
        inserted_count = insert_crime_records(records)
        print(f"Inserted {inserted_count} new records into database")
        
        # Log the fetch
        log_fetch(inserted_count, start_date, end_date)
        
        return True
    else:
        print("No records fetched")
        return False

if __name__ == "__main__":
    # Run initial data fetch
    refresh_crime_data(30)