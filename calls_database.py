import sqlite3
from datetime import datetime
import os

DB_PATH = 'crime_data.db'

def init_calls_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS calls_for_service (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_number TEXT UNIQUE NOT NULL,
            response_date TIMESTAMP NOT NULL,
            priority TEXT NOT NULL,
            problem TEXT NOT NULL,
            service_area TEXT NOT NULL,
            call_type TEXT NOT NULL,
            response_seconds INTEGER,
            weekday TEXT,
            disposition_group TEXT,
            disposition_type TEXT,
            postal_code TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_calls_response_date ON calls_for_service(response_date);
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_calls_problem ON calls_for_service(problem);
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_calls_service_area ON calls_for_service(service_area);
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_calls_priority ON calls_for_service(priority);
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_calls_type ON calls_for_service(call_type);
    ''')
    
    conn.commit()
    conn.close()

def insert_call_records(records):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    inserted_count = 0
    for record in records:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO calls_for_service (
                    incident_number, response_date, priority, problem,
                    call_type, service_area, response_seconds, weekday,
                    disposition_group, disposition_type, postal_code
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                record['Master_Incident_Number'],
                record['Response_Date'],
                record['Priority'],
                record['Problem'],
                record['Type'],
                record['Service_Area'],
                record.get('Seconds', None),
                record.get('Weekday', ''),
                record.get('Disposition_Groups', ''),
                record.get('Disposition_Type', ''),
                record.get('Postal_Code', 'Unknown')
            ))
            if cursor.rowcount > 0:
                inserted_count += 1
        except Exception as e:
            print(f"Error inserting call record {record.get('Master_Incident_Number')}: {e}")
    
    conn.commit()
    conn.close()
    return inserted_count

def get_calls_stats(days=30):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    stats = {}
    
    # Get the most recent date in the database
    cursor.execute('SELECT MAX(date(response_date)) FROM calls_for_service')
    max_date_result = cursor.fetchone()
    
    if max_date_result and max_date_result[0]:
        # Calculate date range based on most recent data
        cursor.execute('''
            SELECT COUNT(*) FROM calls_for_service 
            WHERE date(response_date) >= date(?, '-' || ? || ' days')
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('SELECT COUNT(*) FROM calls_for_service')
    
    stats['total_calls'] = cursor.fetchone()[0]
    
    # Calls by problem type (top 10)
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT problem, COUNT(*) as count 
            FROM calls_for_service 
            WHERE date(response_date) >= date(?, '-' || ? || ' days')
            GROUP BY problem 
            ORDER BY count DESC 
            LIMIT 10
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT problem, COUNT(*) as count 
            FROM calls_for_service 
            GROUP BY problem 
            ORDER BY count DESC 
            LIMIT 10
        ''')
    stats['calls_by_problem'] = cursor.fetchall()
    
    # Calls by priority
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT priority, COUNT(*) as count 
            FROM calls_for_service 
            WHERE date(response_date) >= date(?, '-' || ? || ' days')
            GROUP BY priority 
            ORDER BY priority
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT priority, COUNT(*) as count 
            FROM calls_for_service 
            GROUP BY priority 
            ORDER BY priority
        ''')
    stats['calls_by_priority'] = cursor.fetchall()
    
    # Calls by type (Emergency vs Non-Emergency)
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT call_type, COUNT(*) as count 
            FROM calls_for_service 
            WHERE date(response_date) >= date(?, '-' || ? || ' days')
            GROUP BY call_type 
            ORDER BY count DESC
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT call_type, COUNT(*) as count 
            FROM calls_for_service 
            GROUP BY call_type 
            ORDER BY count DESC
        ''')
    stats['calls_by_type'] = cursor.fetchall()
    
    # Calls by service area
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT service_area, COUNT(*) as count 
            FROM calls_for_service 
            WHERE date(response_date) >= date(?, '-' || ? || ' days')
            GROUP BY service_area 
            ORDER BY count DESC
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT service_area, COUNT(*) as count 
            FROM calls_for_service 
            GROUP BY service_area 
            ORDER BY count DESC
        ''')
    stats['calls_by_area'] = cursor.fetchall()
    
    # Top zip codes
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT postal_code, COUNT(*) as count 
            FROM calls_for_service 
            WHERE date(response_date) >= date(?, '-' || ? || ' days')
            AND postal_code NOT LIKE '%Out of%' 
            AND postal_code != 'Unknown'
            GROUP BY postal_code 
            ORDER BY count DESC 
            LIMIT 10
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT postal_code, COUNT(*) as count 
            FROM calls_for_service 
            WHERE postal_code NOT LIKE '%Out of%' 
            AND postal_code != 'Unknown'
            GROUP BY postal_code 
            ORDER BY count DESC 
            LIMIT 10
        ''')
    stats['top_zip_codes'] = cursor.fetchall()
    
    # Daily trend
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT date(response_date) as date, COUNT(*) as count 
            FROM calls_for_service 
            WHERE date(response_date) >= date(?, '-' || ? || ' days')
            GROUP BY date(response_date) 
            ORDER BY date(response_date)
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT date(response_date) as date, COUNT(*) as count 
            FROM calls_for_service 
            GROUP BY date(response_date) 
            ORDER BY date(response_date)
        ''')
    stats['daily_trend'] = cursor.fetchall()
    
    # Emergency calls count
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT COUNT(*) FROM calls_for_service 
            WHERE date(response_date) >= date(?, '-' || ? || ' days')
            AND call_type = 'Emergency'
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT COUNT(*) FROM calls_for_service 
            WHERE call_type = 'Emergency'
        ''')
    stats['emergency_calls'] = cursor.fetchone()[0]
    
    # Average response time (for calls with response time data)
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT AVG(response_seconds) FROM calls_for_service 
            WHERE date(response_date) >= date(?, '-' || ? || ' days')
            AND response_seconds IS NOT NULL AND response_seconds > 0
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT AVG(response_seconds) FROM calls_for_service 
            WHERE response_seconds IS NOT NULL AND response_seconds > 0
        ''')
    avg_seconds = cursor.fetchone()[0]
    stats['avg_response_minutes'] = round(avg_seconds / 60, 1) if avg_seconds else None
    
    # Calls by disposition type
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT disposition_type, COUNT(*) as count 
            FROM calls_for_service 
            WHERE date(response_date) >= date(?, '-' || ? || ' days')
            AND disposition_type != ''
            GROUP BY disposition_type 
            ORDER BY count DESC
            LIMIT 5
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT disposition_type, COUNT(*) as count 
            FROM calls_for_service 
            WHERE disposition_type != ''
            GROUP BY disposition_type 
            ORDER BY count DESC
            LIMIT 5
        ''')
    stats['calls_by_disposition'] = cursor.fetchall()
    
    conn.close()
    return stats

def get_calls_list(page=1, per_page=100, filters=None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Base query
    query = '''
        SELECT incident_number, response_date, priority, problem, 
               call_type, service_area, postal_code, disposition_type,
               response_seconds, weekday
        FROM calls_for_service
        WHERE 1=1
    '''
    params = []
    
    # Apply filters
    if filters:
        if filters.get('problem'):
            query += ' AND problem = ?'
            params.append(filters['problem'])
        
        if filters.get('priority'):
            query += ' AND priority = ?'
            params.append(filters['priority'])
            
        if filters.get('call_type'):
            query += ' AND call_type = ?'
            params.append(filters['call_type'])
            
        if filters.get('service_area'):
            query += ' AND service_area = ?'
            params.append(filters['service_area'])
        
        if filters.get('postal_code'):
            query += ' AND postal_code = ?'
            params.append(filters['postal_code'])
        
        if filters.get('date_from'):
            query += ' AND date(response_date) >= ?'
            params.append(filters['date_from'])
        
        if filters.get('date_to'):
            query += ' AND date(response_date) <= ?'
            params.append(filters['date_to'])
        
        if filters.get('search'):
            query += ' AND problem LIKE ?'
            search_term = f'%{filters["search"]}%'
            params.append(search_term)
    
    # Count total records
    count_query = f'SELECT COUNT(*) FROM ({query})'
    cursor.execute(count_query, params)
    total_count = cursor.fetchone()[0]
    
    # Add ordering and pagination
    query += ' ORDER BY response_date DESC, incident_number DESC'
    query += ' LIMIT ? OFFSET ?'
    params.extend([per_page, (page - 1) * per_page])
    
    cursor.execute(query, params)
    calls = cursor.fetchall()
    
    conn.close()
    
    return {
        'calls': calls,
        'total': total_count,
        'page': page,
        'per_page': per_page,
        'total_pages': (total_count + per_page - 1) // per_page
    }

def get_calls_filter_options():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get unique problems (top 50 most common)
    cursor.execute('''
        SELECT problem, COUNT(*) as count 
        FROM calls_for_service 
        GROUP BY problem 
        ORDER BY count DESC 
        LIMIT 50
    ''')
    problems = [row[0] for row in cursor.fetchall()]
    
    # Get unique priorities
    cursor.execute('SELECT DISTINCT priority FROM calls_for_service ORDER BY priority')
    priorities = [row[0] for row in cursor.fetchall()]
    
    # Get call types
    cursor.execute('SELECT DISTINCT call_type FROM calls_for_service ORDER BY call_type')
    call_types = [row[0] for row in cursor.fetchall()]
    
    # Get unique service areas
    cursor.execute('SELECT DISTINCT service_area FROM calls_for_service ORDER BY service_area')
    service_areas = [row[0] for row in cursor.fetchall()]
    
    # Get unique zip codes
    cursor.execute('''
        SELECT DISTINCT postal_code FROM calls_for_service 
        WHERE postal_code NOT LIKE '%Out of%' AND postal_code != 'Unknown'
        ORDER BY postal_code
    ''')
    postal_codes = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    return {
        'problems': problems,
        'priorities': priorities,
        'call_types': call_types,
        'service_areas': service_areas,
        'postal_codes': postal_codes
    }