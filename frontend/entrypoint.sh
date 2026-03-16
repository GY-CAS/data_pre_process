#!/bin/sh
set -e

BACKEND_URL=${BACKEND_SERVICE_URL:-http://127.0.0.1:8000}

sed -i "s|__BACKEND_URL__|$BACKEND_URL|g" /app/dist/index.html

exec npm run preview -- --host 0.0.0.0 --port 13000
