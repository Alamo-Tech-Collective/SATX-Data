import sqlite3
from datetime import datetime, timedelta
import pytz
from us_crime_severity_weights import get_us_weighted_severity

DB_PATH = 'crime_data.db'
CST = pytz.timezone('America/Chicago')

def get_multi_period_insights():
    """Get insights for 30, 60, and 90 day periods with proper rate-based trends"""
    periods = [30, 60, 90]
    all_insights = {}
    
    for period in periods:
        all_insights[period] = get_combined_insights(period)
    
    # Calculate RATE-BASED trends between periods
    # Since CSI is already normalized per day, we compare the daily rates directly
    
    # Compare 30-day rate to 60-day rate
    csi_30 = all_insights[30]['safety_components']['crime_severity_index']
    csi_60 = all_insights[60]['safety_components']['crime_severity_index']
    if csi_60 > 0:
        # Positive trend means crime is up (bad), negative means crime is down (good)
        trend_30_vs_60 = ((csi_30 - csi_60) / csi_60) * 100
        all_insights[30]['csi_trend'] = trend_30_vs_60
    
    # Compare 60-day rate to 90-day rate
    csi_90 = all_insights[90]['safety_components']['crime_severity_index']
    if csi_90 > 0:
        trend_60_vs_90 = ((csi_60 - csi_90) / csi_90) * 100
        all_insights[60]['csi_trend'] = trend_60_vs_90
    
    # For 90-day, use the internal recent trend which compares to historical average
    all_insights[90]['csi_trend'] = all_insights[90]['safety_components'].get('recent_trend', 0)
    
    # Also calculate daily crime rate trends for better comparison
    for period in periods:
        daily_crime_rate = all_insights[period]['total_crimes'] / period
        all_insights[period]['daily_crime_rate'] = round(daily_crime_rate, 1)
        
        daily_arrest_rate = all_insights[period]['total_arrests'] / period
        all_insights[period]['daily_arrest_rate'] = round(daily_arrest_rate, 1)
    
    # Calculate daily rate trends
    if all_insights[60]['daily_crime_rate'] > 0:
        crime_trend_30_vs_60 = ((all_insights[30]['daily_crime_rate'] - all_insights[60]['daily_crime_rate']) / all_insights[60]['daily_crime_rate']) * 100
        all_insights[30]['crime_rate_trend'] = round(crime_trend_30_vs_60, 1)
    
    if all_insights[90]['daily_crime_rate'] > 0:
        crime_trend_60_vs_90 = ((all_insights[60]['daily_crime_rate'] - all_insights[90]['daily_crime_rate']) / all_insights[90]['daily_crime_rate']) * 100
        all_insights[60]['crime_rate_trend'] = round(crime_trend_60_vs_90, 1)
        
        # 90-day trend: compare last 30 days to prior 60 days within the 90-day window
        last_30_rate = all_insights[30]['daily_crime_rate']
        prior_60_rate = (all_insights[90]['total_crimes'] - all_insights[30]['total_crimes']) / 60 if all_insights[90]['total_crimes'] > all_insights[30]['total_crimes'] else 0
        if prior_60_rate > 0:
            crime_trend_recent = ((last_30_rate - prior_60_rate) / prior_60_rate) * 100
            all_insights[90]['crime_rate_trend'] = round(crime_trend_recent, 1)
    
    return all_insights

def get_combined_insights(days=30):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    insights = {}
    
    # Get the most recent dates from each table
    cursor.execute('SELECT MAX(report_date) FROM crimes')
    crime_max_date = cursor.fetchone()[0]
    
    cursor.execute('SELECT MAX(report_date) FROM arrests')
    arrest_max_date = cursor.fetchone()[0]
    
    cursor.execute('SELECT MAX(date(response_date)) FROM calls_for_service')
    calls_max_date = cursor.fetchone()[0]
    
    # Overall Public Safety Metrics
    if crime_max_date:
        cursor.execute('''
            SELECT COUNT(*) FROM crimes 
            WHERE report_date >= date(?, '-' || ? || ' days')
        ''', (crime_max_date, days-1))
        total_crimes = cursor.fetchone()[0]
    else:
        total_crimes = 0
    
    if arrest_max_date:
        cursor.execute('''
            SELECT COUNT(*) FROM arrests 
            WHERE report_date >= date(?, '-' || ? || ' days')
        ''', (arrest_max_date, days-1))
        total_arrests = cursor.fetchone()[0]
    else:
        total_arrests = 0
    
    if calls_max_date:
        cursor.execute('''
            SELECT COUNT(*) FROM calls_for_service 
            WHERE date(response_date) >= date(?, '-' || ? || ' days')
        ''', (calls_max_date, days-1))
        total_calls = cursor.fetchone()[0]
    else:
        total_calls = 0
    
    insights['total_incidents'] = total_crimes + total_arrests
    insights['total_crimes'] = total_crimes
    insights['total_arrests'] = total_arrests
    insights['total_calls'] = total_calls
    
    # Response Efficiency - removed due to limited calls data
    insights['calls_to_incidents_ratio'] = 0
    
    # Arrest to Crime Ratio (moved up)
    insights['arrest_rate'] = (total_arrests / total_crimes * 100) if total_crimes > 0 else 0
    
    # Combined Service Area Analysis
    area_data = {}
    
    if crime_max_date:
        cursor.execute('''
            SELECT service_area, COUNT(*) as count 
            FROM crimes 
            WHERE report_date >= date(?, '-' || ? || ' days')
            GROUP BY service_area
        ''', (crime_max_date, days-1))
        for area, count in cursor.fetchall():
            if area not in area_data:
                area_data[area] = {'crimes': 0, 'arrests': 0, 'calls': 0}
            area_data[area]['crimes'] = count
    
    if arrest_max_date:
        cursor.execute('''
            SELECT service_area, COUNT(*) as count 
            FROM arrests 
            WHERE report_date >= date(?, '-' || ? || ' days')
            GROUP BY service_area
        ''', (arrest_max_date, days-1))
        for area, count in cursor.fetchall():
            if area not in area_data:
                area_data[area] = {'crimes': 0, 'arrests': 0, 'calls': 0}
            area_data[area]['arrests'] = count
    
    # Removed calls data from area analysis due to limited data availability
    
    # Calculate area scores (higher score = more activity)
    for area in area_data:
        area_data[area]['total'] = (
            area_data[area]['crimes'] + 
            area_data[area]['arrests']
        )
        area_data[area]['calls'] = 0  # Set to 0 for compatibility
    
    insights['area_analysis'] = sorted(
        [(area, data) for area, data in area_data.items()],
        key=lambda x: x[1]['total'],
        reverse=True
    )[:7]  # Top 7 areas
    
    # Time Analysis - Hour of Day Pattern (for calls)
    if calls_max_date:
        cursor.execute('''
            SELECT strftime('%H', response_date) as hour, COUNT(*) as count
            FROM calls_for_service
            WHERE date(response_date) >= date(?, '-' || ? || ' days')
            GROUP BY hour
            ORDER BY hour
        ''', (calls_max_date, days-1))
        insights['hourly_pattern'] = cursor.fetchall()
    else:
        insights['hourly_pattern'] = []
    
    # High-Risk Zip Codes (combined metric)
    zip_scores = {}
    
    if crime_max_date:
        cursor.execute('''
            SELECT zip_code, COUNT(*) * 3 as weighted_count
            FROM crimes 
            WHERE report_date >= date(?, '-' || ? || ' days')
            AND zip_code NOT LIKE '%Out of%' AND zip_code != 'Unknown'
            GROUP BY zip_code
        ''', (crime_max_date, days-1))
        for zip_code, score in cursor.fetchall():
            zip_scores[zip_code] = zip_scores.get(zip_code, 0) + score
    
    if arrest_max_date:
        cursor.execute('''
            SELECT zip_code, COUNT(*) * 2 as weighted_count
            FROM arrests 
            WHERE report_date >= date(?, '-' || ? || ' days')
            AND zip_code NOT LIKE '%Out of%' AND zip_code != 'Unknown'
            GROUP BY zip_code
        ''', (arrest_max_date, days-1))
        for zip_code, score in cursor.fetchall():
            zip_scores[zip_code] = zip_scores.get(zip_code, 0) + score
    
    # Removed calls data from zip code analysis due to limited data availability
    
    insights['high_risk_zips'] = sorted(
        [(zip_code, score) for zip_code, score in zip_scores.items()],
        key=lambda x: x[1],
        reverse=True
    )[:10]
    
    # Trending Crime Types (30-day comparison)
    if crime_max_date:
        # Check how many days of data we actually have
        cursor.execute('''
            SELECT COUNT(DISTINCT report_date) as days_available
            FROM crimes
            WHERE report_date <= ?
        ''', (crime_max_date,))
        days_available = cursor.fetchone()[0]
        
        if days_available >= 60:
            # We have enough data for a proper 30-day comparison
            cursor.execute('''
                WITH recent AS (
                    SELECT crime_type, COUNT(*) as recent_count
                    FROM crimes
                    WHERE report_date > date(?, '-30 days')
                    AND report_date <= ?
                    GROUP BY crime_type
                ),
                previous AS (
                    SELECT crime_type, COUNT(*) as prev_count
                    FROM crimes
                    WHERE report_date > date(?, '-60 days')
                    AND report_date <= date(?, '-30 days')
                    GROUP BY crime_type
                )
                SELECT 
                    COALESCE(r.crime_type, p.crime_type) as crime_type,
                    COALESCE(r.recent_count, 0) as recent,
                    COALESCE(p.prev_count, 0) as previous,
                    CASE 
                        WHEN COALESCE(p.prev_count, 0) = 0 THEN 
                            CASE WHEN COALESCE(r.recent_count, 0) > 0 THEN 100 ELSE 0 END
                        ELSE ((COALESCE(r.recent_count, 0) - COALESCE(p.prev_count, 0)) * 100.0 / 
                              COALESCE(p.prev_count, 0))
                    END as change_percent
                FROM recent r
                FULL OUTER JOIN previous p ON r.crime_type = p.crime_type
                WHERE COALESCE(r.recent_count, 0) >= 10  -- Minimum threshold for significance
                ORDER BY ABS(change_percent) DESC  -- Sort by absolute change to show biggest movers
                LIMIT 10
            ''', (crime_max_date, crime_max_date, crime_max_date, crime_max_date))
            
            trending_crimes = []
            for row in cursor.fetchall():
                trending_crimes.append({
                    'type': row[0],
                    'recent': row[1],
                    'previous': row[2],
                    'change': round(row[3], 1)
                })
            insights['trending_crimes'] = trending_crimes
        else:
            # Not enough data for comparison, just show top crime types from available data
            cursor.execute('''
                SELECT crime_type, COUNT(*) as count
                FROM crimes
                WHERE report_date > date(?, '-' || ? || ' days')
                AND report_date <= ?
                GROUP BY crime_type
                HAVING COUNT(*) >= 10
                ORDER BY count DESC
                LIMIT 10
            ''', (crime_max_date, min(days_available-1, 29), crime_max_date))
            
            trending_crimes = []
            for row in cursor.fetchall():
                trending_crimes.append({
                    'type': row[0],
                    'recent': row[1],
                    'previous': None,  # Use None to indicate no data
                    'change': None
                })
            insights['trending_crimes'] = trending_crimes
    else:
        insights['trending_crimes'] = []
    
    # Crime Severity Index using weighted methodology
    # Based on research into Canadian CSI and UK Crime Harm Index
    
    population = 1500000  # San Antonio population
    
    # Calculate weighted crime severity
    total_weighted_severity = 0
    violent_weighted_severity = 0
    property_weighted_severity = 0
    
    if crime_max_date:
        # Get all crimes with their types and categories
        cursor.execute('''
            SELECT crime_type, crime_against, COUNT(*) as count
            FROM crimes 
            WHERE report_date >= date(?, '-' || ? || ' days')
            GROUP BY crime_type, crime_against
        ''', (crime_max_date, days-1))
        
        crime_counts = cursor.fetchall()
        
        for crime_type, crime_against, count in crime_counts:
            severity = get_us_weighted_severity(crime_type, crime_against)
            weighted_value = severity * count
            total_weighted_severity += weighted_value
            
            if crime_against and crime_against.upper() == 'PERSON':
                violent_weighted_severity += weighted_value
            else:
                property_weighted_severity += weighted_value
    
    # Calculate the Crime Severity Index (per 100k population)
    # Similar to Canadian CSI formula but scaled down by factor of 100
    crime_severity_index = (total_weighted_severity / population) * 100000 / days * 365 / 100
    violent_csi = (violent_weighted_severity / population) * 100000 / days * 365 / 100
    property_csi = (property_weighted_severity / population) * 100000 / days * 365 / 100
    
    # Calculate trend (weighted severity comparison)
    recent_trend_weighted = 0
    if crime_max_date:
        # Last 7 days weighted severity
        cursor.execute('''
            SELECT crime_type, crime_against, COUNT(*) as count
            FROM crimes 
            WHERE report_date >= date(?, '-6 days')
            GROUP BY crime_type, crime_against
        ''', (crime_max_date,))
        
        recent_severity = 0
        for crime_type, crime_against, count in cursor.fetchall():
            recent_severity += get_us_weighted_severity(crime_type, crime_against) * count
        
        # Previous period weighted severity
        cursor.execute('''
            SELECT crime_type, crime_against, COUNT(*) as count
            FROM crimes 
            WHERE report_date >= date(?, '-' || ? || ' days')
            AND report_date < date(?, '-6 days')
            GROUP BY crime_type, crime_against
        ''', (crime_max_date, days-1, crime_max_date))
        
        prev_severity = 0
        for crime_type, crime_against, count in cursor.fetchall():
            prev_severity += get_us_weighted_severity(crime_type, crime_against) * count
        
        if prev_severity > 0:
            expected_weekly_severity = (prev_severity / (days - 7)) * 7
            recent_trend_weighted = ((recent_severity - expected_weekly_severity) / expected_weekly_severity) * 100
    
    # Normalize CSI to 0-100 scale for display
    # For San Antonio data, observed range is roughly 0-15000
    # We'll use a logarithmic scale for better visualization
    import math
    if crime_severity_index > 0:
        # Log scale: 100 = 1, 1000 = 2, 10000 = 3, 100000 = 4
        log_value = math.log10(max(1, crime_severity_index))
        # Map log(100) to log(100000) => 2 to 5 => 0 to 100
        display_score = min(100, max(0, (log_value - 2) / 3 * 100))
    else:
        display_score = 0
    
    insights['safety_score'] = round(display_score, 1)
    insights['safety_components'] = {
        'crime_severity_index': round(crime_severity_index, 1),
        'violent_csi': round(violent_csi, 1),
        'property_csi': round(property_csi, 1),
        'recent_trend': round(recent_trend_weighted, 1),
        'total_crimes': total_crimes,
        'daily_rate': round(total_crimes / days, 1)
    }
    
    # Daily trends for all three metrics
    combined_daily = {}
    
    if crime_max_date:
        cursor.execute('''
            SELECT report_date, COUNT(*) 
            FROM crimes 
            WHERE report_date >= date(?, '-' || ? || ' days')
            GROUP BY report_date
        ''', (crime_max_date, days-1))
        for date, count in cursor.fetchall():
            if date not in combined_daily:
                combined_daily[date] = {'crimes': 0, 'arrests': 0, 'calls': 0}
            combined_daily[date]['crimes'] = count
    
    if arrest_max_date:
        cursor.execute('''
            SELECT report_date, COUNT(*) 
            FROM arrests 
            WHERE report_date >= date(?, '-' || ? || ' days')
            GROUP BY report_date
        ''', (arrest_max_date, days-1))
        for date, count in cursor.fetchall():
            if date not in combined_daily:
                combined_daily[date] = {'crimes': 0, 'arrests': 0, 'calls': 0}
            combined_daily[date]['arrests'] = count
    
    # Set calls to 0 for all dates for compatibility
    for date in combined_daily:
        combined_daily[date]['calls'] = 0
    
    insights['daily_combined'] = sorted(combined_daily.items())
    
    # Key Insights Text
    insights['key_findings'] = generate_key_findings(insights)
    
    conn.close()
    return insights

def generate_key_findings(insights):
    findings = {
        'improvements': [],
        'concerns': [],
        'overview': []
    }
    
    # === IMPROVEMENTS SECTION ===
    
    # Arrest rate insight
    if insights['arrest_rate'] > 20:
        findings['improvements'].append({
            'type': 'positive',
            'text': f"Strong law enforcement presence with {insights['arrest_rate']:.1f}% arrest rate"
        })
    
    # === OVERVIEW SECTION ===
    
    # High activity areas
    if insights['area_analysis']:
        top_area = insights['area_analysis'][0]
        findings['overview'].append({
            'type': 'info',
            'text': f"{top_area[0]} area has highest activity with {top_area[1]['total']:,} total incidents"
        })
    
    # Low arrest rate concern
    if insights['arrest_rate'] < 10:
        findings['concerns'].append({
            'type': 'warning',
            'text': f"Low arrest rate ({insights['arrest_rate']:.1f}%) may indicate enforcement challenges"
        })
    
    # === CONCERNS SECTION ===
    
    # Trending crimes - significant increases
    if insights['trending_crimes']:
        for crime in insights['trending_crimes'][:10]:
            if crime.get('change') and crime['change'] > 50:
                findings['concerns'].append({
                    'type': 'alert',
                    'text': f"{crime['type']} increased {crime['change']:.0f}% in the last 30 days"
                })
    
    # Back to improvements - significant decreases
    if insights['trending_crimes']:
        for crime in insights['trending_crimes'][:10]:
            if crime.get('change') and crime['change'] < -20:
                findings['improvements'].append({
                    'type': 'positive',
                    'text': f"{crime['type']} decreased {abs(crime['change']):.0f}% in the last 30 days"
                })
    
    # Crime severity index insights
    csi = insights['safety_components']['crime_severity_index']
    if csi < 100:
        findings['improvements'].append({
            'type': 'positive',
            'text': f"Crime Severity Index of {csi:.1f} indicates relatively low crime impact"
        })
    elif csi > 300:
        findings['concerns'].append({
            'type': 'warning',
            'text': f"Crime Severity Index of {csi:.1f} indicates elevated crime severity"
        })
    
    # Violent vs Property crime balance
    violent_csi = insights['safety_components']['violent_csi']
    property_csi = insights['safety_components']['property_csi']
    if violent_csi > property_csi:
        findings['concerns'].append({
            'type': 'alert',
            'text': f"Violent crimes contributing more to severity (Index: {violent_csi:.1f}) than property crimes"
        })
    
    # Severity trend
    trend = insights['safety_components']['recent_trend']
    if trend > 20:
        findings['concerns'].append({
            'type': 'alert',
            'text': f"Crime severity has increased {trend:.1f}% in the past week"
        })
    elif trend < -20:
        findings['improvements'].append({
            'type': 'positive',
            'text': f"Crime severity has decreased {abs(trend):.1f}% in the past week"
        })
    
    # Daily crime rate insights
    daily_rate = insights['safety_components']['daily_rate']
    if daily_rate < 300:
        findings['improvements'].append({
            'type': 'positive',
            'text': f"Average of {daily_rate:.0f} crimes per day is below major city average"
        })
    
    # Look for low-crime areas (positive finding)
    if insights['area_analysis'] and len(insights['area_analysis']) > 3:
        low_activity_areas = [area for area in insights['area_analysis'][-3:] if area[1]['total'] < 500]
        if low_activity_areas:
            findings['improvements'].append({
                'type': 'positive',
                'text': f"{low_activity_areas[0][0]} area shows low crime activity with only {low_activity_areas[0][1]['total']} incidents"
            })
    
    # Check for improving arrest rates or crime categories
    if insights.get('total_crimes', 0) > 0:
        violent_ratio = insights.get('violent_crimes', 0) / insights['total_crimes'] * 100
        if violent_ratio < 15:
            findings['improvements'].append({
                'type': 'positive',
                'text': f"Violent crimes comprise only {violent_ratio:.1f}% of total crime"
            })
    
    # Add overview stats
    findings['overview'].append({
        'type': 'info',
        'text': f"Total of {insights['total_incidents']:,} incidents reported in analysis period"
    })
    
    
    return findings