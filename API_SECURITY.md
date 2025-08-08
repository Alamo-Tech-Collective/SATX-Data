# API Security Documentation

## Overview

The Crime Dashboard API is protected by multiple layers of security to prevent unauthorized access and abuse.

## Security Features

### 1. API Key Authentication
- All API endpoints require a valid API key
- Keys are hashed using SHA-256 before storage
- Keys cannot be retrieved after generation

### 2. Rate Limiting
- Default: 100 requests per minute per API key
- Violations result in temporary IP blocking (5 minutes)
- Health check endpoint: 10 requests per minute

### 3. IP Allowlisting
- By default, only localhost and private networks are allowed
- API key authentication bypasses IP restrictions
- Configure additional IPs via ALLOWED_IPS environment variable

### 4. CORS Protection
- Only configured origins can access the API from browsers
- Default: localhost only
- Configure production domains in app.py

### 5. Security Headers
- X-Content-Type-Options: nosniff
- X-Frame-Options: DENY
- X-XSS-Protection: 1; mode=block
- Strict-Transport-Security: max-age=31536000
- Content-Security-Policy: default-src 'self'

## API Key Management

### Generating an API Key

#### Method 1: Command Line Script
```bash
python generate_api_key.py "Description of client"
```

#### Method 2: Admin API Endpoint (localhost only)
```bash
curl -X POST http://localhost:5001/admin/generate-api-key \
  -H "Content-Type: application/json" \
  -d '{"description": "Mobile App Client"}'
```

### Using API Keys

Include the API key in your requests using either:

1. **HTTP Header (Recommended)**
   ```
   X-API-Key: your-api-key-here
   ```

2. **Query Parameter**
   ```
   ?api_key=your-api-key-here
   ```

### Revoking API Keys

Delete the corresponding line from `api_keys.txt` or implement a revocation endpoint.

## Protected Endpoints

All `/api/*` endpoints require authentication:
- `/api/stats` - Crime statistics
- `/api/crimes` - Crime records
- `/api/arrests` - Arrest records
- `/api/calls` - Service calls

Public endpoints (no auth required):
- `/api/health` - Health check (rate limited)
- `/` - Web dashboard (HTML views)
- `/crime-dashboard` - Crime dashboard view
- `/arrests-dashboard` - Arrests dashboard view
- `/calls-dashboard` - Calls dashboard view

## Configuration

### Environment Variables

Create a `.env` file based on `.env.example`:

```bash
# Allowed IP addresses or networks (comma-separated)
ALLOWED_IPS=192.168.1.0/24,10.0.0.0/8

# Flask configuration
FLASK_ENV=production
SECRET_KEY=your-secret-key-here
```

### Testing the API

1. Generate an API key:
   ```bash
   python generate_api_key.py "Test Client"
   ```

2. Test an endpoint:
   ```bash
   curl -H "X-API-Key: your-key-here" \
        http://localhost:5001/api/stats?days=30
   ```

## Security Best Practices

1. **Never commit API keys** to version control
2. **Use HTTPS** in production
3. **Rotate keys** regularly
4. **Monitor** API usage for anomalies
5. **Implement logging** for security events
6. **Use environment variables** for sensitive configuration
7. **Keep dependencies updated** for security patches

## Troubleshooting

### 401 Unauthorized
- Check if API key is included in request
- Verify API key is valid (check api_keys.txt)

### 403 Forbidden
- IP address not in allowlist
- Check ALLOWED_IPS environment variable

### 429 Too Many Requests
- Rate limit exceeded
- Wait 60 seconds or reduce request frequency

## Production Deployment

1. Set `FLASK_ENV=production`
2. Use a proper WSGI server (Gunicorn, uWSGI)
3. Configure HTTPS with SSL certificate
4. Set up reverse proxy (nginx/Apache)
5. Enable firewall rules
6. Configure log monitoring
7. Set up automated backups of api_keys.txt

## Example Client Implementation

### Python
```python
import requests

API_KEY = "your-api-key-here"
BASE_URL = "http://localhost:5001"

headers = {"X-API-Key": API_KEY}

# Get crime statistics
response = requests.get(f"{BASE_URL}/api/stats", headers=headers, params={"days": 30})
data = response.json()
```

### JavaScript
```javascript
const API_KEY = 'your-api-key-here';
const BASE_URL = 'http://localhost:5001';

fetch(`${BASE_URL}/api/stats?days=30`, {
  headers: {
    'X-API-Key': API_KEY
  }
})
.then(response => response.json())
.then(data => console.log(data));
```

### cURL
```bash
curl -H "X-API-Key: your-api-key-here" \
     "http://localhost:5001/api/stats?days=30"
```