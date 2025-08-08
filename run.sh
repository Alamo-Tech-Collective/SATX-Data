#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Check for --refresh flag
if [ "$1" = "--refresh" ]; then
    echo "Force refreshing database..."
    python app.py --refresh
else
    # Run the Flask application normally
    echo "Starting San Antonio Crime Dashboard..."
    python app.py
fi