#!/usr/bin/env python3
"""
Generate API key for the Crime Dashboard API

Usage:
    python generate_api_key.py [description]
    
Example:
    python generate_api_key.py "Mobile App Client"
"""

import sys
import os
import secrets
import hashlib

def generate_key(description=""):
    """Generate a new API key"""
    key = secrets.token_urlsafe(32)
    key_hash = hashlib.sha256(key.encode()).hexdigest()
    
    # Save to file
    with open('api_keys.txt', 'a') as f:
        f.write(f"{key_hash}|{description}\n")
    
    return key

def main():
    description = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Default API Key"
    
    # Generate new key
    api_key = generate_key(description)
    
    print("\n" + "="*60)
    print("API KEY GENERATED SUCCESSFULLY")
    print("="*60)
    print(f"\nAPI Key: {api_key}")
    print(f"Description: {description}")
    print("\n⚠️  IMPORTANT: Store this key securely!")
    print("This key cannot be retrieved again.")
    print("\nTo use this key, include it in your API requests:")
    print("  - Header: X-API-Key: <your-key>")
    print("  - Query: ?api_key=<your-key>")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()