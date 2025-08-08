from flask import Flask, render_template, jsonify, request, redirect
from flask_cors import CORS
from database import init_database, get_crime_stats, get_last_fetch_info, get_crimes_list, get_filter_options
from arrests_database import get_arrest_stats, get_arrests_list, get_arrest_filter_options
from calls_database import get_calls_stats, get_calls_list, get_calls_filter_options
from insights import get_combined_insights, get_multi_period_insights
from fetch_data import refresh_crime_data
from fetch_arrests import refresh_arrests_data
from fetch_calls import refresh_calls_data
from scheduler import start_scheduler_thread, scheduled_refresh
from security import require_api_key, rate_limit, ip_restrict, secure_headers, api_key_manager, get_client_ip
from datetime import datetime
import pytz
import json
import os
import sys

app = Flask(__name__)

# Configure CORS - restrict to specific origins
CORS(app, origins=[
    'http://localhost:*',
    'http://127.0.0.1:*',
    # Add your production domains here
], supports_credentials=True)

# Apply security headers to all responses
@app.after_request
def apply_security_headers(response):
    return secure_headers(response)

# CST timezone
CST = pytz.timezone('America/Chicago')

@app.route('/')
def index():
    # Get multi-period insights for 30, 60, and 90 days
    all_insights = get_multi_period_insights()
    last_fetch = get_last_fetch_info()
    
    # Use 30-day insights as the primary display
    insights = all_insights[30]
    
    # Add absolute values to trending crimes
    if insights.get('trending_crimes'):
        for crime in insights['trending_crimes']:
            crime['change_abs'] = abs(crime['change'])
    
    # Format last fetch time
    if last_fetch and last_fetch['fetch_date']:
        fetch_dt = datetime.fromisoformat(last_fetch['fetch_date'].replace(' ', 'T'))
        if fetch_dt.tzinfo is None:
            fetch_dt = pytz.utc.localize(fetch_dt)
        cst_time = fetch_dt.astimezone(CST)
        last_fetch['fetch_date_formatted'] = cst_time.strftime('%B %d, %Y at %I:%M %p CST')
    
    # Prepare chart data (using 30-day data for most charts)
    area_labels = [area[0] for area in insights['area_analysis']]
    area_crimes = [area[1]['crimes'] for area in insights['area_analysis']]
    area_arrests = [area[1]['arrests'] for area in insights['area_analysis']]
    
    hourly_labels = [f"{int(hour[0])}:00" for hour in insights['hourly_pattern']]
    hourly_data = [hour[1] for hour in insights['hourly_pattern']]
    
    # Prepare multi-period daily trend data
    daily_labels_30 = [day[0] for day in all_insights[30]['daily_combined']]
    daily_crimes_30 = [day[1]['crimes'] for day in all_insights[30]['daily_combined']]
    daily_arrests_30 = [day[1]['arrests'] for day in all_insights[30]['daily_combined']]
    
    # Get average daily values for 60 and 90 days
    avg_daily_crimes_60 = all_insights[60]['total_crimes'] / 60 if all_insights[60]['total_crimes'] > 0 else 0
    avg_daily_crimes_90 = all_insights[90]['total_crimes'] / 90 if all_insights[90]['total_crimes'] > 0 else 0
    avg_daily_arrests_60 = all_insights[60]['total_arrests'] / 60 if all_insights[60]['total_arrests'] > 0 else 0
    avg_daily_arrests_90 = all_insights[90]['total_arrests'] / 90 if all_insights[90]['total_arrests'] > 0 else 0
    
    zip_labels = [zip_data[0] for zip_data in insights['high_risk_zips'][:5]]
    zip_scores = [zip_data[1] for zip_data in insights['high_risk_zips'][:5]]
    
    return render_template('insights.html',
                         insights=insights,
                         all_insights=all_insights,
                         last_fetch=last_fetch,
                         current_date=datetime.now(CST).strftime('%B %d, %Y'),
                         area_labels=area_labels,
                         area_crimes=area_crimes,
                         area_arrests=area_arrests,
                         hourly_labels=hourly_labels,
                         hourly_data=hourly_data,
                         daily_labels_30=daily_labels_30,
                         daily_crimes_30=daily_crimes_30,
                         daily_arrests_30=daily_arrests_30,
                         avg_daily_crimes_60=avg_daily_crimes_60,
                         avg_daily_crimes_90=avg_daily_crimes_90,
                         avg_daily_arrests_60=avg_daily_arrests_60,
                         avg_daily_arrests_90=avg_daily_arrests_90,
                         zip_labels=zip_labels,
                         zip_scores=zip_scores)

@app.route('/crime-dashboard')
def dashboard():
    # Get statistics for last 30 days
    stats = get_crime_stats(30)
    last_fetch = get_last_fetch_info()
    
    # Calculate percentages for crime categories
    if stats['total_crimes'] > 0:
        category_percentages = {
            cat: (count / stats['total_crimes']) * 100 
            for cat, count in stats['crimes_by_category']
        }
        
        violent_percentage = (stats['violent_crimes'] / stats['total_crimes']) * 100
    else:
        category_percentages = {}
        violent_percentage = 0
    
    # Prepare data for charts
    crime_type_labels = [crime[0] for crime in stats['crimes_by_type'][:10]]
    crime_type_data = [crime[1] for crime in stats['crimes_by_type'][:10]]
    
    category_labels = [cat[0] for cat in stats['crimes_by_category']]
    category_data = [cat[1] for cat in stats['crimes_by_category']]
    
    area_labels = [area[0] for area in stats['crimes_by_area']]
    area_data = [area[1] for area in stats['crimes_by_area']]
    
    trend_labels = [trend[0] for trend in stats['daily_trend']]
    trend_data = [trend[1] for trend in stats['daily_trend']]
    
    # Format last fetch time in CST 12-hour format
    if last_fetch and last_fetch['fetch_date']:
        # Parse the fetch date and convert to CST
        fetch_dt = datetime.fromisoformat(last_fetch['fetch_date'].replace(' ', 'T'))
        if fetch_dt.tzinfo is None:
            fetch_dt = pytz.utc.localize(fetch_dt)
        cst_time = fetch_dt.astimezone(CST)
        last_fetch['fetch_date_formatted'] = cst_time.strftime('%B %d, %Y at %I:%M %p CST')
    
    return render_template('dashboard.html',
                         stats=stats,
                         last_fetch=last_fetch,
                         category_percentages=category_percentages,
                         violent_percentage=violent_percentage,
                         current_date=datetime.now(CST).strftime('%B %d, %Y'),
                         crime_type_labels=crime_type_labels,
                         crime_type_data=crime_type_data,
                         category_labels=category_labels,
                         category_data=category_data,
                         area_labels=area_labels,
                         area_data=area_data,
                         trend_labels=trend_labels,
                         trend_data=trend_data)

@app.route('/api/stats')
@require_api_key
@rate_limit(max_requests=100, window=60)
def api_stats():
    # Get days parameter (default to 30)
    days = request.args.get('days', 30, type=int)
    if days not in [30, 60, 90]:
        days = 30
    
    stats = get_crime_stats(days)
    
    # Convert to JSON-serializable format
    return jsonify({
        'total_crimes': stats['total_crimes'],
        'violent_crimes': stats['violent_crimes'],
        'crimes_by_type': [{'type': t, 'count': c} for t, c in stats['crimes_by_type']],
        'crimes_by_category': [{'category': cat, 'count': c} for cat, c in stats['crimes_by_category']],
        'crimes_by_area': [{'area': a, 'count': c} for a, c in stats['crimes_by_area']],
        'top_zip_codes': [{'zip': z, 'count': c} for z, c in stats['top_zip_codes']],
        'daily_trend': [{'date': d, 'count': c} for d, c in stats['daily_trend']]
    })

@app.route('/crimes')
def crimes_list():
    # Get query parameters
    page = request.args.get('page', 1, type=int)
    days = request.args.get('days', 30, type=int)
    if days not in [30, 60, 90]:
        days = 30
    crime_type = request.args.get('crime_type', '')
    service_area = request.args.get('service_area', '')
    zip_code = request.args.get('zip_code', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    search = request.args.get('search', '')
    
    # Build filters
    filters = {}
    if crime_type:
        filters['crime_type'] = crime_type
    if service_area:
        filters['service_area'] = service_area
    if zip_code:
        filters['zip_code'] = zip_code
    if date_from:
        filters['date_from'] = date_from
    if date_to:
        filters['date_to'] = date_to
    if search:
        filters['search'] = search
    
    # Get crimes and filter options
    result = get_crimes_list(page=page, per_page=100, filters=filters)
    filter_options = get_filter_options()
    
    # Calculate pagination range
    start_page = max(1, page - 2)
    end_page = min(result['total_pages'] + 1, page + 3)
    page_range = list(range(start_page, end_page))
    
    return render_template('crimes_list.html',
                         crimes=result['crimes'],
                         total=result['total'],
                         page=result['page'],
                         total_pages=result['total_pages'],
                         filters=filters,
                         filter_options=filter_options,
                         page_range=page_range,
                         days=days)

@app.route('/api/crimes')
@require_api_key
@rate_limit(max_requests=100, window=60)
def api_crimes():
    # Get query parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 100, type=int)
    
    # Build filters from query params
    filters = {}
    for key in ['crime_type', 'service_area', 'zip_code', 'date_from', 'date_to', 'search']:
        value = request.args.get(key)
        if value:
            filters[key] = value
    
    result = get_crimes_list(page=page, per_page=per_page, filters=filters)
    
    # Convert to JSON-friendly format
    crimes_data = []
    for crime in result['crimes']:
        crimes_data.append({
            'report_id': crime[0],
            'report_date': crime[1],
            'crime_type': crime[2],
            'crime_against': crime[3],
            'service_area': crime[4],
            'zip_code': crime[5],
            'nibrs_group': crime[6]
        })
    
    return jsonify({
        'crimes': crimes_data,
        'total': result['total'],
        'page': result['page'],
        'per_page': result['per_page'],
        'total_pages': result['total_pages']
    })

@app.route('/arrests-dashboard')
def arrests_dashboard():
    # Get statistics for last 30 days
    stats = get_arrest_stats(30)
    last_fetch = get_last_fetch_info()
    
    # Calculate percentages for severity categories
    if stats['total_arrests'] > 0:
        severity_percentages = {
            sev: (count / stats['total_arrests']) * 100 
            for sev, count in stats['arrests_by_severity']
        }
        
        felony_percentage = (stats['felony_arrests'] / stats['total_arrests']) * 100
    else:
        severity_percentages = {}
        felony_percentage = 0
    
    # Prepare data for charts
    offense_labels = [offense[0] for offense in stats['arrests_by_offense'][:10]]
    offense_data = [offense[1] for offense in stats['arrests_by_offense'][:10]]
    
    severity_labels = [sev[0] for sev in stats['arrests_by_severity']]
    severity_data = [sev[1] for sev in stats['arrests_by_severity']]
    
    area_labels = [area[0] for area in stats['arrests_by_area']]
    area_data = [area[1] for area in stats['arrests_by_area']]
    
    trend_labels = [trend[0] for trend in stats['daily_trend']]
    trend_data = [trend[1] for trend in stats['daily_trend']]
    
    # Format last fetch time in CST 12-hour format
    if last_fetch and last_fetch['fetch_date']:
        # Parse the fetch date and convert to CST
        fetch_dt = datetime.fromisoformat(last_fetch['fetch_date'].replace(' ', 'T'))
        if fetch_dt.tzinfo is None:
            fetch_dt = pytz.utc.localize(fetch_dt)
        cst_time = fetch_dt.astimezone(CST)
        last_fetch['fetch_date_formatted'] = cst_time.strftime('%B %d, %Y at %I:%M %p CST')
    
    return render_template('arrests_dashboard.html',
                         stats=stats,
                         last_fetch=last_fetch,
                         severity_percentages=severity_percentages,
                         felony_percentage=felony_percentage,
                         current_date=datetime.now(CST).strftime('%B %d, %Y'),
                         offense_labels=offense_labels,
                         offense_data=offense_data,
                         severity_labels=severity_labels,
                         severity_data=severity_data,
                         area_labels=area_labels,
                         area_data=area_data,
                         trend_labels=trend_labels,
                         trend_data=trend_data)

@app.route('/arrests')
def arrests_list():
    # Get query parameters
    page = request.args.get('page', 1, type=int)
    days = request.args.get('days', 30, type=int)
    if days not in [30, 60, 90]:
        days = 30
    offense = request.args.get('offense', '')
    severity = request.args.get('severity', '')
    service_area = request.args.get('service_area', '')
    zip_code = request.args.get('zip_code', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    search = request.args.get('search', '')
    
    # Build filters
    filters = {}
    if offense:
        filters['offense'] = offense
    if severity:
        filters['severity'] = severity
    if service_area:
        filters['service_area'] = service_area
    if zip_code:
        filters['zip_code'] = zip_code
    if date_from:
        filters['date_from'] = date_from
    if date_to:
        filters['date_to'] = date_to
    if search:
        filters['search'] = search
    
    # Get arrests and filter options
    result = get_arrests_list(page=page, per_page=100, filters=filters)
    filter_options = get_arrest_filter_options()
    
    # Calculate pagination range
    start_page = max(1, page - 2)
    end_page = min(result['total_pages'] + 1, page + 3)
    page_range = list(range(start_page, end_page))
    
    return render_template('arrests_list.html',
                         arrests=result['arrests'],
                         total=result['total'],
                         page=result['page'],
                         total_pages=result['total_pages'],
                         filters=filters,
                         filter_options=filter_options,
                         days=days,
                         page_range=page_range)

@app.route('/api/arrests')
@require_api_key
@rate_limit(max_requests=100, window=60)
def api_arrests():
    # Get query parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 100, type=int)
    
    # Build filters from query params
    filters = {}
    for key in ['offense', 'severity', 'service_area', 'zip_code', 'date_from', 'date_to', 'search']:
        value = request.args.get(key)
        if value:
            filters[key] = value
    
    result = get_arrests_list(page=page, per_page=per_page, filters=filters)
    
    # Convert to JSON-friendly format
    arrests_data = []
    for arrest in result['arrests']:
        arrests_data.append({
            'report_id': arrest[0],
            'report_date': arrest[1],
            'person_id': arrest[2],
            'offense': arrest[3],
            'severity': arrest[4],
            'service_area': arrest[5],
            'zip_code': arrest[6],
            'report_month': arrest[7]
        })
    
    return jsonify({
        'arrests': arrests_data,
        'total': result['total'],
        'page': result['page'],
        'per_page': result['per_page'],
        'total_pages': result['total_pages']
    })

@app.route('/calls-dashboard')
def calls_dashboard():
    # Get statistics for last 30 days
    stats = get_calls_stats(30)
    last_fetch = get_last_fetch_info()
    
    # Calculate percentages
    if stats['total_calls'] > 0:
        emergency_percentage = (stats['emergency_calls'] / stats['total_calls']) * 100
    else:
        emergency_percentage = 0
    
    # Prepare data for charts
    problem_labels = [prob[0] for prob in stats['calls_by_problem'][:10]]
    problem_data = [prob[1] for prob in stats['calls_by_problem'][:10]]
    
    priority_labels = [pri[0] for pri in stats['calls_by_priority']]
    priority_data = [pri[1] for pri in stats['calls_by_priority']]
    
    type_labels = [t[0] for t in stats['calls_by_type']]
    type_data = [t[1] for t in stats['calls_by_type']]
    
    area_labels = [area[0] for area in stats['calls_by_area']]
    area_data = [area[1] for area in stats['calls_by_area']]
    
    trend_labels = [trend[0] for trend in stats['daily_trend']]
    trend_data = [trend[1] for trend in stats['daily_trend']]
    
    disposition_labels = [disp[0] for disp in stats['calls_by_disposition']]
    disposition_data = [disp[1] for disp in stats['calls_by_disposition']]
    
    # Format last fetch time in CST 12-hour format
    if last_fetch and last_fetch['fetch_date']:
        # Parse the fetch date and convert to CST
        fetch_dt = datetime.fromisoformat(last_fetch['fetch_date'].replace(' ', 'T'))
        if fetch_dt.tzinfo is None:
            fetch_dt = pytz.utc.localize(fetch_dt)
        cst_time = fetch_dt.astimezone(CST)
        last_fetch['fetch_date_formatted'] = cst_time.strftime('%B %d, %Y at %I:%M %p CST')
    
    return render_template('calls_dashboard.html',
                         stats=stats,
                         last_fetch=last_fetch,
                         emergency_percentage=emergency_percentage,
                         current_date=datetime.now(CST).strftime('%B %d, %Y'),
                         problem_labels=problem_labels,
                         problem_data=problem_data,
                         priority_labels=priority_labels,
                         priority_data=priority_data,
                         type_labels=type_labels,
                         type_data=type_data,
                         area_labels=area_labels,
                         area_data=area_data,
                         trend_labels=trend_labels,
                         trend_data=trend_data,
                         disposition_labels=disposition_labels,
                         disposition_data=disposition_data)

@app.route('/calls')
def calls_list():
    # Get query parameters
    page = request.args.get('page', 1, type=int)
    days = request.args.get('days', 30, type=int)
    if days not in [30, 60, 90]:
        days = 30
    problem = request.args.get('problem', '')
    priority = request.args.get('priority', '')
    call_type = request.args.get('call_type', '')
    service_area = request.args.get('service_area', '')
    postal_code = request.args.get('postal_code', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    search = request.args.get('search', '')
    
    # Build filters
    filters = {}
    if problem:
        filters['problem'] = problem
    if priority:
        filters['priority'] = priority
    if call_type:
        filters['call_type'] = call_type
    if service_area:
        filters['service_area'] = service_area
    if postal_code:
        filters['postal_code'] = postal_code
    if date_from:
        filters['date_from'] = date_from
    if date_to:
        filters['date_to'] = date_to
    if search:
        filters['search'] = search
    
    # Get calls and filter options
    result = get_calls_list(page=page, per_page=100, filters=filters)
    filter_options = get_calls_filter_options()
    
    # Calculate pagination range
    start_page = max(1, page - 2)
    end_page = min(result['total_pages'] + 1, page + 3)
    page_range = list(range(start_page, end_page))
    
    return render_template('calls_list.html',
                         calls=result['calls'],
                         total=result['total'],
                         page=result['page'],
                         total_pages=result['total_pages'],
                         filters=filters,
                         filter_options=filter_options,
                         page_range=page_range,
                         days=days)

@app.route('/admin/generate-api-key', methods=['POST'])
@ip_restrict  # Only allow from trusted IPs
def generate_api_key():
    """Generate a new API key (admin only)"""
    client_ip = get_client_ip()
    
    # Extra security: only allow from localhost
    if client_ip not in ['127.0.0.1', '::1']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Get description from request
    data = request.get_json() or {}
    description = data.get('description', f'Generated at {datetime.now(CST)}')
    
    # Generate new key
    new_key = api_key_manager.generate_key(description)
    
    return jsonify({
        'api_key': new_key,
        'description': description,
        'message': 'Store this key securely. It cannot be retrieved again.'
    })

@app.route('/api/health')
@rate_limit(max_requests=10, window=60)
def health_check():
    """Health check endpoint (no auth required for monitoring)"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now(CST).isoformat()
    })

@app.route('/api/calls')
@require_api_key
@rate_limit(max_requests=100, window=60)
def api_calls():
    # Get query parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 100, type=int)
    
    # Build filters from query params
    filters = {}
    for key in ['problem', 'priority', 'call_type', 'service_area', 'postal_code', 'date_from', 'date_to', 'search']:
        value = request.args.get(key)
        if value:
            filters[key] = value
    
    result = get_calls_list(page=page, per_page=per_page, filters=filters)
    
    # Convert to JSON-friendly format
    calls_data = []
    for call in result['calls']:
        calls_data.append({
            'incident_number': call[0],
            'response_date': call[1],
            'priority': call[2],
            'problem': call[3],
            'call_type': call[4],
            'service_area': call[5],
            'postal_code': call[6],
            'disposition_type': call[7],
            'response_seconds': call[8],
            'weekday': call[9]
        })
    
    return jsonify({
        'calls': calls_data,
        'total': result['total'],
        'page': result['page'],
        'per_page': result['per_page'],
        'total_pages': result['total_pages']
    })

if __name__ == '__main__':
    # Initialize database on startup
    init_database()
    
    # Check for --refresh flag
    force_refresh = '--refresh' in sys.argv
    
    if force_refresh:
        print("\n========================================")
        print("MANUAL REFRESH TRIGGERED")
        print("========================================")
        # Use the exact same function as the scheduled refresh
        scheduled_refresh()
        print("========================================\n")
    else:
        # Check if database is empty and inform user
        crime_stats = get_crime_stats(90)
        arrest_stats = get_arrest_stats(90)
        calls_stats = get_calls_stats(90)
        
        if crime_stats['total_crimes'] == 0:
            print("\nNote: Crime database is empty. Use './run.sh --refresh' to fetch data.")
        
        if arrest_stats['total_arrests'] == 0:
            print("Note: Arrests database is empty. Use './run.sh --refresh' to fetch data.")
        
        if calls_stats['total_calls'] == 0:
            print("Note: Calls database is empty. Use './run.sh --refresh' to fetch data.")
    
    # Start the scheduler for daily updates
    start_scheduler_thread()
    
    # Run the app
    app.run(debug=True, port=5001, host='0.0.0.0')