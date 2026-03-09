#!/bin/bash
# Production server. Install: pip install gunicorn
cd "$(dirname "$0")"
port="${SCRAPER_SERVER_PORT:-8080}"
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
exec gunicorn -w 1 -b "0.0.0.0:$port" --timeout 0 server:app
