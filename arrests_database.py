import sqlite3
from datetime import datetime
import os

DB_PATH = 'crime_data.db'

def init_arrests_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS arrests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id TEXT UNIQUE NOT NULL,
            report_date DATE NOT NULL,
            person_id TEXT NOT NULL,
            offense TEXT NOT NULL,
            severity TEXT NOT NULL,
            service_area TEXT NOT NULL,
            report_month TEXT,
            zip_code TEXT,
            datetime_occurred TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_arrests_report_date ON arrests(report_date);
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_arrests_offense ON arrests(offense);
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_arrests_service_area ON arrests(service_area);
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_arrests_severity ON arrests(severity);
    ''')
    
    conn.commit()
    conn.close()

def insert_arrest_records(records):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    inserted_count = 0
    for record in records:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO arrests (
                    report_id, report_date, person_id, offense,
                    severity, service_area, report_month, zip_code, datetime_occurred
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                record['Report_ID'],
                record['Report_Date'],
                record['Person'],
                record['Offense'],
                record['Severity'],
                record['Service_Area'],
                record.get('Report_Month', ''),
                record.get('Zip_Code', 'Unknown'),
                record.get('DateTime', record['Report_Date'])
            ))
            if cursor.rowcount > 0:
                inserted_count += 1
        except Exception as e:
            print(f"Error inserting arrest record {record.get('Report_ID')}: {e}")
    
    conn.commit()
    conn.close()
    return inserted_count

def get_arrest_stats(days=30):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    stats = {}
    
    # Get the most recent date in the database
    cursor.execute('SELECT MAX(report_date) FROM arrests')
    max_date_result = cursor.fetchone()
    
    if max_date_result and max_date_result[0]:
        # Calculate date range based on most recent data
        cursor.execute('''
            SELECT COUNT(*) FROM arrests 
            WHERE report_date >= date(?, '-' || ? || ' days')
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('SELECT COUNT(*) FROM arrests')
    
    stats['total_arrests'] = cursor.fetchone()[0]
    
    # Arrests by offense type
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT offense, COUNT(*) as count 
            FROM arrests 
            WHERE report_date >= date(?, '-' || ? || ' days')
            GROUP BY offense 
            ORDER BY count DESC 
            LIMIT 10
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT offense, COUNT(*) as count 
            FROM arrests 
            GROUP BY offense 
            ORDER BY count DESC 
            LIMIT 10
        ''')
    stats['arrests_by_offense'] = cursor.fetchall()
    
    # Arrests by severity
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT severity, COUNT(*) as count 
            FROM arrests 
            WHERE report_date >= date(?, '-' || ? || ' days')
            GROUP BY severity 
            ORDER BY count DESC
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT severity, COUNT(*) as count 
            FROM arrests 
            GROUP BY severity 
            ORDER BY count DESC
        ''')
    stats['arrests_by_severity'] = cursor.fetchall()
    
    # Arrests by service area
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT service_area, COUNT(*) as count 
            FROM arrests 
            WHERE report_date >= date(?, '-' || ? || ' days')
            GROUP BY service_area 
            ORDER BY count DESC
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT service_area, COUNT(*) as count 
            FROM arrests 
            GROUP BY service_area 
            ORDER BY count DESC
        ''')
    stats['arrests_by_area'] = cursor.fetchall()
    
    # Top zip codes
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT zip_code, COUNT(*) as count 
            FROM arrests 
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
            FROM arrests 
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
            FROM arrests 
            WHERE report_date >= date(?, '-' || ? || ' days')
            GROUP BY report_date 
            ORDER BY report_date
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT report_date, COUNT(*) as count 
            FROM arrests 
            GROUP BY report_date 
            ORDER BY report_date
        ''')
    stats['daily_trend'] = cursor.fetchall()
    
    # Felony arrests count
    if max_date_result and max_date_result[0]:
        cursor.execute('''
            SELECT COUNT(*) FROM arrests 
            WHERE report_date >= date(?, '-' || ? || ' days')
            AND severity LIKE '%Felony%'
        ''', (max_date_result[0], days-1))
    else:
        cursor.execute('''
            SELECT COUNT(*) FROM arrests 
            WHERE severity LIKE '%Felony%'
        ''')
    stats['felony_arrests'] = cursor.fetchone()[0]
    
    conn.close()
    return stats

def get_arrests_list(page=1, per_page=100, filters=None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Base query
    query = '''
        SELECT report_id, report_date, person_id, offense, 
               severity, service_area, zip_code, report_month
        FROM arrests
        WHERE 1=1
    '''
    params = []
    
    # Apply filters
    if filters:
        if filters.get('offense'):
            query += ' AND offense = ?'
            params.append(filters['offense'])
        
        if filters.get('severity'):
            query += ' AND severity = ?'
            params.append(filters['severity'])
            
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
            query += ' AND offense LIKE ?'
            search_term = f'%{filters["search"]}%'
            params.append(search_term)
    
    # Count total records
    count_query = f'SELECT COUNT(*) FROM ({query})'
    cursor.execute(count_query, params)
    total_count = cursor.fetchone()[0]
    
    # Add ordering and pagination
    query += ' ORDER BY report_date DESC, report_id DESC'
    query += ' LIMIT ? OFFSET ?'
    params.extend([per_page, (page - 1) * per_page])
    
    cursor.execute(query, params)
    arrests = cursor.fetchall()
    
    conn.close()
    
    return {
        'arrests': arrests,
        'total': total_count,
        'page': page,
        'per_page': per_page,
        'total_pages': (total_count + per_page - 1) // per_page
    }

def get_arrest_filter_options():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get unique offenses (top 50 most common)
    cursor.execute('''
        SELECT offense, COUNT(*) as count 
        FROM arrests 
        GROUP BY offense 
        ORDER BY count DESC 
        LIMIT 50
    ''')
    offenses = [row[0] for row in cursor.fetchall()]
    
    # Get unique severities
    cursor.execute('SELECT DISTINCT severity FROM arrests ORDER BY severity')
    severities = [row[0] for row in cursor.fetchall()]
    
    # Get unique service areas
    cursor.execute('SELECT DISTINCT service_area FROM arrests ORDER BY service_area')
    service_areas = [row[0] for row in cursor.fetchall()]
    
    # Get unique zip codes
    cursor.execute('''
        SELECT DISTINCT zip_code FROM arrests 
        WHERE zip_code NOT LIKE '%Out of%' AND zip_code != 'Unknown'
        ORDER BY zip_code
    ''')
    zip_codes = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    return {
        'offenses': offenses,
        'severities': severities,
        'service_areas': service_areas,
        'zip_codes': zip_codes
    }