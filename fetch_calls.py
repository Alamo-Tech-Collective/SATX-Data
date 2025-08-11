import requests
import json
from datetime import datetime, timedelta
from calls_database import init_calls_table, insert_call_records
import time
import pytz

# CST timezone
CST = pytz.timezone('America/Chicago')

API_BASE_URL = "https://data.sanantonio.gov/api/3/action/datastore_search"
RESOURCE_ID = "9cb17985-ac16-49a6-ad69-6fe5ad8f2bf5"
RECORDS_PER_PAGE = 1000

def fetch_calls_data_page(offset=0):
    params = {
        'resource_id': RESOURCE_ID,
        'limit': RECORDS_PER_PAGE,
        'offset': offset,
        'sort': 'Response_Date desc'
    }
    
    try:
        response = requests.get(API_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching calls data: {e}")
        return None

def fetch_all_calls_data(days=30, fetch_all=False):
    if fetch_all:
        # When using --refresh, limit to 180 days max
        days = 180
        print(f"Starting to fetch calls for service data for the last {days} days (--refresh limit)...")
    else:
        print(f"Starting to fetch calls for service data for the last {days} days...")
    
    # Get the most recent data available
    all_records = []
    offset = 0
    total_records = None
    records_needed = days * 500  # Always use days limit now
    
    while len(all_records) < records_needed:
        print(f"Fetching calls records from offset {offset}...")
        data = fetch_calls_data_page(offset)
        
        if not data or not data.get('success'):
            print("Failed to fetch calls data")
            break
        
        result = data.get('result', {})
        records = result.get('records', [])
        
        if not records:
            break
        
        # Just add all records - we'll get the most recent 30 days worth
        all_records.extend(records)
        
        # Check if we have enough days of data
        if all_records:
            # Extract dates from response_date timestamps
            dates = []
            for r in all_records:
                if 'Response_Date' in r and r['Response_Date']:
                    try:
                        # Parse the date portion only
                        date_str = r['Response_Date'].split(' ')[0]
                        dates.append(date_str)
                    except:
                        continue
            
            unique_dates = sorted(set(dates), reverse=True)
            if len(unique_dates) >= days:
                print(f"Fetched {days} days of calls data, stopping")
                # Filter to exactly the most recent 30 days
                cutoff_date = unique_dates[days-1] if len(unique_dates) >= days else unique_dates[-1]
                all_records = [r for r in all_records if r.get('Response_Date', '').startswith(cutoff_date) or r.get('Response_Date', '') > cutoff_date]
                break
        
        # Get total from first request
        if total_records is None:
            total_records = result.get('total', 0)
            print(f"Total calls records available: {total_records}")
        
        # Check if we've fetched all records
        if len(records) < RECORDS_PER_PAGE:
            break
        
        offset += RECORDS_PER_PAGE
        
        # Be nice to the API
        time.sleep(0.5)
    
    print(f"Fetched {len(all_records)} calls records total")
    
    # Get date range
    if all_records:
        dates = []
        for r in all_records:
            if 'Response_Date' in r and r['Response_Date']:
                try:
                    date_str = r['Response_Date'].split(' ')[0]
                    dates.append(date_str)
                except:
                    continue
        
        if dates:
            start_date = min(dates)
            end_date = max(dates)
        else:
            start_date = end_date = datetime.now().strftime('%Y-%m-%d')
    else:
        start_date = end_date = datetime.now().strftime('%Y-%m-%d')
    
    return all_records, start_date, end_date

def refresh_calls_data(days=30, fetch_all=False):
    current_time = datetime.now(CST).strftime('%B %d, %Y at %I:%M %p CST')
    print(f"Refreshing calls for service data - {current_time}")
    
    # Initialize calls table if needed
    init_calls_table()
    
    # Fetch data
    records, start_date, end_date = fetch_all_calls_data(days, fetch_all)
    
    if records:
        # Insert into database
        inserted_count = insert_call_records(records)
        print(f"Inserted {inserted_count} new call records into database")
        
        # Log the fetch in the existing fetch_history table
        from database import log_fetch
        log_fetch(inserted_count, start_date, end_date)
        
        return True
    else:
        print("No call records fetched")
        return False

if __name__ == "__main__":
    # Run initial data fetch
    refresh_calls_data(30)