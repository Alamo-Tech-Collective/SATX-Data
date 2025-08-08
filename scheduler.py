import schedule
import time
from datetime import datetime
from fetch_data import refresh_crime_data
from fetch_arrests import refresh_arrests_data
from fetch_calls import refresh_calls_data
import threading
import pytz

# CST timezone
CST = pytz.timezone('America/Chicago')

def scheduled_refresh():
    current_time = datetime.now(CST).strftime('%I:%M %p CST')
    print(f"Starting scheduled refresh at {current_time}")
    try:
        # Refresh crime data (90 days)
        refresh_crime_data(90)
        print("Crime data refresh completed successfully")
        
        # Refresh arrests data (90 days)
        refresh_arrests_data(90)
        print("Arrests data refresh completed successfully")
        
        # Refresh calls for service data (90 days)
        refresh_calls_data(90)
        print("Calls for service data refresh completed successfully")
        
        print("All scheduled refreshes completed successfully")
    except Exception as e:
        print(f"Error during scheduled refresh: {e}")

def run_scheduler():
    # Schedule daily refresh at 2 AM CST
    schedule.every().day.at("02:00").do(scheduled_refresh)
    
    print("Scheduler started. Will refresh data daily at 2:00 AM CST")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

def start_scheduler_thread():
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    print("Background scheduler thread started")

if __name__ == "__main__":
    # Test the scheduler
    print("Testing scheduler - will run refresh immediately and then wait for scheduled time")
    scheduled_refresh()
    run_scheduler()