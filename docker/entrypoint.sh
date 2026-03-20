#!/bin/sh

echo "========================================="
echo "Frontend Container Startup Script"
echo "========================================="

echo "[1/4] Waiting for backend service to be ready..."

BACKEND_URL="${BACKEND_URL}"
MAX_RETRIES=30
RETRY_INTERVAL=2

if [ -z "$BACKEND_URL" ]; then
    echo "ERROR: BACKEND_URL environment variable is not set!"
    echo "Please set BACKEND_URL in .env.docker file"
    echo "Example: BACKEND_URL=http://host.docker.internal:8006"
    exit 1
fi

echo "Backend URL: $BACKEND_URL"

for i in $(seq 1 $MAX_RETRIES); do
    HEALTH_URL="${BACKEND_URL}/health"
    if curl -sf "$HEALTH_URL" > /dev/null 2>&1; then
        echo "[OK] Backend service is ready!"
        break
    fi
    echo "  Attempt $i/$MAX_RETRIES: Backend not ready, waiting ${RETRY_INTERVAL}s..."
    sleep $RETRY_INTERVAL
    
    if [ $i -eq $MAX_RETRIES ]; then
        echo "ERROR: Backend service did not become ready in time"
        echo "Continuing anyway - frontend will show connection errors..."
    fi
done

echo "[2/4] Checking static files..."
if [ ! -d "/app/dist" ]; then
    echo "ERROR: Static files directory /app/dist not found!"
    exit 1
fi
echo "  Static files: OK"

echo "[3/4] Generating proxy configuration..."

PROXY_CONFIG_FILE="/app/proxy-config.json"
cat > "$PROXY_CONFIG_FILE" << EOF
{
    "backendUrl": "$BACKEND_URL"
}
EOF

echo "  Proxy config: $PROXY_CONFIG_FILE"
echo "  Backend URL: $BACKEND_URL"

echo "[4/4] Starting frontend server..."
echo "========================================="
echo "Frontend Configuration:"
echo "  Port: ${FRONTEND_PORT:-13000}"
echo "  API Target: $BACKEND_URL"
echo "========================================="

export API_TARGET="$BACKEND_URL"

exec node /app/frontend-server.js
