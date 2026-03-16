#!/bin/bash
echo "=========================================="
echo "  Database Connection Diagnostic Script"
echo "=========================================="

echo ""
echo "[1] Checking host.docker.internal resolution..."
ping -c 2 host.docker.internal 2>/dev/null && echo "✓ host.docker.internal resolved" || echo "✗ host.docker.internal not resolved"

echo ""
echo "[2] Checking MySQL connection (port 3306)..."
timeout 5 bash -c "echo > /dev/tcp/host.docker.internal/3306" 2>/dev/null && echo "✓ MySQL port 3306 is reachable" || echo "✗ MySQL port 3306 is NOT reachable"

echo ""
echo "[3] Checking ClickHouse connection (port 9000)..."
timeout 5 bash -c "echo > /dev/tcp/host.docker.internal/9000" 2>/dev/null && echo "✓ ClickHouse port 9000 is reachable" || echo "✗ ClickHouse port 9000 is NOT reachable"

echo ""
echo "[4] Checking ClickHouse HTTP (port 8123)..."
timeout 5 bash -c "echo > /dev/tcp/host.docker.internal/8123" 2>/dev/null && echo "✓ ClickHouse HTTP port 8123 is reachable" || echo "✗ ClickHouse HTTP port 8123 is NOT reachable"

echo ""
echo "[5] Checking MinIO connection (port 9000)..."
timeout 5 bash -c "echo > /dev/tcp/host.docker.internal/9000" 2>/dev/null && echo "✓ MinIO port 9000 is reachable" || echo "✗ MinIO port 9000 is NOT reachable"

echo ""
echo "[6] Environment Variables Check..."
echo "MYSQL_HOST: ${MYSQL_HOST:-not set}"
echo "MYSQL_PORT: ${MYSQL_PORT:-not set}"
echo "CK_HOST: ${CK_HOST:-not set}"
echo "CK_PORT: ${CK_PORT:-not set}"
echo "MINIO_ENDPOINT: ${MINIO_ENDPOINT:-not set}"

echo ""
echo "=========================================="
echo "  Diagnostic Complete"
echo "=========================================="
