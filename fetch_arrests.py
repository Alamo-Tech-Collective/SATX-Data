import requests
import json
from datetime import datetime, timedelta
from arrests_database import init_arrests_table, insert_arrest_records
import time
import pytz

# CST timezone
CST = pytz.timezone('America/Chicago')

API_BASE_URL = "https://data.sanantonio.gov/api/3/action/datastore_search"
RESOURCE_ID = "5bf98f1b-25c2-488c-aba7-082d7f8d38aa"
RECORDS_PER_PAGE = 1000

def fetch_arrests_data_page(offset=0):
    params = {
        'resource_id': RESOURCE_ID,
        'limit': RECORDS_PER_PAGE,
        'offset': offset,
        'sort': 'Report_Date desc'
    }
    
    try:
        response = requests.get(API_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching arrests data: {e}")
        return None

def fetch_all_arrests_data(days=30):
    print(f"Starting to fetch arrests data for the last {days} days...")
    
    # Get the most recent data available
    all_records = []
    offset = 0
    total_records = None
    records_needed = days * 100  # Estimate ~100 arrests per day
    
    while len(all_records) < records_needed:
        print(f"Fetching arrests records from offset {offset}...")
        data = fetch_arrests_data_page(offset)
        
        if not data or not data.get('success'):
            print("Failed to fetch arrests data")
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
                print(f"Fetched {days} days of arrests data, stopping")
                # Filter to exactly the most recent 30 days
                cutoff_date = dates[days-1] if len(dates) >= days else dates[-1]
                all_records = [r for r in all_records if r.get('Report_Date', '') >= cutoff_date]
                break
        
        # Get total from first request
        if total_records is None:
            total_records = result.get('total', 0)
            print(f"Total arrests records available: {total_records}")
        
        # Check if we've fetched all records
        if len(records) < RECORDS_PER_PAGE:
            break
        
        offset += RECORDS_PER_PAGE
        
        # Be nice to the API
        time.sleep(0.5)
    
    print(f"Fetched {len(all_records)} arrests records total")
    
    # Get date range
    if all_records:
        dates = [r['Report_Date'] for r in all_records if 'Report_Date' in r]
        start_date = min(dates) if dates else datetime.now().strftime('%Y-%m-%d')
        end_date = max(dates) if dates else datetime.now().strftime('%Y-%m-%d')
    else:
        start_date = end_date = datetime.now().strftime('%Y-%m-%d')
    
    return all_records, start_date, end_date

def refresh_arrests_data(days=30):
    current_time = datetime.now(CST).strftime('%B %d, %Y at %I:%M %p CST')
    print(f"Refreshing arrests data - {current_time}")
    
    # Initialize arrests table if needed
    init_arrests_table()
    
    # Fetch data
    records, start_date, end_date = fetch_all_arrests_data(days)
    
    if records:
        # Insert into database
        inserted_count = insert_arrest_records(records)
        print(f"Inserted {inserted_count} new arrest records into database")
        
        # Log the fetch in the existing fetch_history table
        from database import log_fetch
        log_fetch(inserted_count, start_date, end_date)
        
        return True
    else:
        print("No arrest records fetched")
        return False

if __name__ == "__main__":
    # Run initial data fetch
    refresh_arrests_data(30)