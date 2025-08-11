import sqlite3
from datetime import datetime
import os
from config import DB_PATH

def init_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crimes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id TEXT UNIQUE NOT NULL,
            report_date DATE NOT NULL,
            crime_type TEXT NOT NULL,
            crime_against TEXT NOT NULL,
            service_area TEXT NOT NULL,
            zip_code TEXT,
            nibrs_group TEXT,
            datetime_occurred TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_report_date ON crimes(report_date);
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_crime_type ON crimes(crime_type);
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_service_area ON crimes(service_area);
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fetch_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            records_fetched INTEGER,
            date_range_start DATE,
            date_range_end DATE
        )
    ''')
    
    conn.commit()
    conn.close()
    
    # Also initialize arrests table
    from arrests_database import init_arrests_table
    init_arrests_table()
    
    # Also initialize calls table
    from calls_database import init_calls_table
    init_calls_table()

def insert_crime_records(records):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    inserted_count = 0
    for record in records:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO crimes (
                    report_id, report_date, crime_type, crime_against,
                    service_area, zip_code, nibrs_group, datetime_occurred
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                record['Report_ID'],
                record['Report_Date'],
                record['NIBRS_Code_Name'],
                record['NIBRS_Crime_Against'],
                record['Service_Area'],
                record.get('Zip_Code', 'Unknown'),
                record.get('NIBRS_Group', ''),
                record.get('DateTime', record['Report_Date'])
            ))
            if cursor.rowcount > 0:
                inserted_count += 1
        except Exception as e:
            print(f"Error inserting record {record.get('Report_ID')}: {e}")
    
    conn.commit()
    conn.close()
    return inserted_count

def log_fetch(records_count, start_date, end_date):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO fetch_history (records_fetched, date_range_start, date_range_end)
        VALUES (?, ?, ?)
    ''', (records_count, start_date, end_date))
    
    conn.commit()
    conn.close()

def get_crime_stats(days=30):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    stats = {}
    
    # Get the most recent date in the database
    cursor.execute('SELECT MAX(report_date) FROM crimes')
    max_date_result = cursor.fetchone()
    
    if max_date_result and max_date_result[0]:
        # Calculate date range based on most recent data
        cursor.execute('''
            SELECT COUNT(*) FROM crimes 
            WHERE report_date >= date(?, '-' || ? || ' days')
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('SELECT COUNT(*) FROM crimes')
    
    stats['total_crimes'] = cursor.fetchone()[0]
    
    # Crimes by type
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT crime_type, COUNT(*) as count 
            FROM crimes 
            WHERE report_date >= date(?, '-' || ? || ' days')
            GROUP BY crime_type 
            ORDER BY count DESC 
            LIMIT 10
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT crime_type, COUNT(*) as count 
            FROM crimes 
            GROUP BY crime_type 
            ORDER BY count DESC 
            LIMIT 10
        ''')
    stats['crimes_by_type'] = cursor.fetchall()
    
    # Crimes by category
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT crime_against, COUNT(*) as count 
            FROM crimes 
            WHERE report_date >= date(?, '-' || ? || ' days')
            GROUP BY crime_against 
            ORDER BY count DESC
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT crime_against, COUNT(*) as count 
            FROM crimes 
            GROUP BY crime_against 
            ORDER BY count DESC
        ''')
    stats['crimes_by_category'] = cursor.fetchall()
    
    # Crimes by service area
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT service_area, COUNT(*) as count 
            FROM crimes 
            WHERE report_date >= date(?, '-' || ? || ' days')
            GROUP BY service_area 
            ORDER BY count DESC
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT service_area, COUNT(*) as count 
            FROM crimes 
            GROUP BY service_area 
            ORDER BY count DESC
        ''')
    stats['crimes_by_area'] = cursor.fetchall()
    
    # Top zip codes
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT zip_code, COUNT(*) as count 
            FROM crimes 
            WHERE report_date >= date(?, '-' || ? || ' days')
            AND zip_code NOT LIKE '%Out of%' 
            AND zip_code != 'Unknown'
            GROUP BY zip_code 
            ORDER BY count DESC 
            LIMIT 10
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT zip_code, COUNT(*) as count 
            FROM crimes 
            WHERE zip_code NOT LIKE '%Out of%' 
            AND zip_code != 'Unknown'
            GROUP BY zip_code 
            ORDER BY count DESC 
            LIMIT 10
        ''')
    stats['top_zip_codes'] = cursor.fetchall()
    
    # Daily trend
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT report_date, COUNT(*) as count 
            FROM crimes 
            WHERE report_date >= date(?, '-' || ? || ' days')
            GROUP BY report_date 
            ORDER BY report_date
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT report_date, COUNT(*) as count 
            FROM crimes 
            GROUP BY report_date 
            ORDER BY report_date
        ''')
    stats['daily_trend'] = cursor.fetchall()
    
    # Violent crimes count
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT COUNT(*) FROM crimes 
            WHERE report_date >= date(?, '-' || ? || ' days')
            AND (crime_type LIKE '%Assault%' OR crime_type LIKE '%Rape%' 
                 OR crime_type LIKE '%Robbery%' OR crime_type LIKE '%Homicide%')
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT COUNT(*) FROM crimes 
            WHERE (crime_type LIKE '%Assault%' OR crime_type LIKE '%Rape%' 
                 OR crime_type LIKE '%Robbery%' OR crime_type LIKE '%Homicide%')
        ''')
    stats['violent_crimes'] = cursor.fetchone()[0]
    
    conn.close()
    return stats

def get_last_fetch_info():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT fetch_date, records_fetched, date_range_start, date_range_end
        FROM fetch_history 
        ORDER BY fetch_date DESC 
        LIMIT 1
    ''')
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return {
            'fetch_date': result[0],
            'records_fetched': result[1],
            'date_range_start': result[2],
            'date_range_end': result[3]
        }
    return None

def get_crimes_list(page=1, per_page=100, filters=None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Base query
    query = '''
        SELECT report_id, report_date, crime_type, crime_against, 
               service_area, zip_code, nibrs_group
        FROM crimes
        WHERE 1=1
    '''
    params = []
    
    # Apply filters
    if filters:
        if filters.get('crime_type'):
            query += ' AND crime_type = ?'
            params.append(filters['crime_type'])
        
        if filters.get('service_area'):
            query += ' AND service_area = ?'
            params.append(filters['service_area'])
        
        if filters.get('zip_code'):
            query += ' AND zip_code = ?'
            params.append(filters['zip_code'])
        
        if filters.get('date_from'):
            query += ' AND report_date >= ?'
            params.append(filters['date_from'])
        
        if filters.get('date_to'):
            query += ' AND report_date <= ?'
            params.append(filters['date_to'])
        
        if filters.get('search'):
            query += ' AND (crime_type LIKE ? OR nibrs_group LIKE ?)'
            search_term = f'%{filters["search"]}%'
            params.extend([search_term, search_term])
    
    # Count total records
    count_query = f'SELECT COUNT(*) FROM ({query})'
    cursor.execute(count_query, params)
    total_count = cursor.fetchone()[0]
    
    # Add ordering and pagination
    query += ' ORDER BY report_date DESC, report_id DESC'
    query += ' LIMIT ? OFFSET ?'
    params.extend([per_page, (page - 1) * per_page])
    
    cursor.execute(query, params)
    crimes = cursor.fetchall()
    
    conn.close()
    
    return {
        'crimes': crimes,
        'total': total_count,
        'page': page,
        'per_page': per_page,
        'total_pages': (total_count + per_page - 1) // per_page
    }

def get_filter_options():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get unique crime types
    cursor.execute('SELECT DISTINCT crime_type FROM crimes ORDER BY crime_type')
    crime_types = [row[0] for row in cursor.fetchall()]
    
    # Get unique service areas
    cursor.execute('SELECT DISTINCT service_area FROM crimes ORDER BY service_area')
    service_areas = [row[0] for row in cursor.fetchall()]
    
    # Get unique zip codes
    cursor.execute('''
        SELECT DISTINCT zip_code FROM crimes 
        WHERE zip_code NOT LIKE '%Out of%' AND zip_code != 'Unknown'
        ORDER BY zip_code
    ''')
    zip_codes = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    return {
        'crime_types': crime_types,
        'service_areas': service_areas,
        'zip_codes': zip_codes
    }