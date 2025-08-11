import os

def get_db_path():
    """
    Returns the appropriate database path based on environment.
    Uses /app/data/ when running in Docker, current directory otherwise.
    """
    # Check if we're running in Docker (common indicators)
    if os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER'):
        return '/app/data/crime_data.db'
    else:
        return 'crime_data.db'

# Single source of truth for database path
DB_PATH = get_db_path()
