#!/bin/bash

set -e

echo "========================================="
echo "Docker Compose 容器化应用自动化测试"
echo "========================================="

BACKEND_URL="${BACKEND_URL:-http://localhost:8005}"
FRONTEND_URL="${FRONTEND_URL:-http://localhost:13005}"
MAX_RETRIES=30
RETRY_INTERVAL=5

check_service() {
    local name=$1
    local url=$2
    local endpoint=$3
    
    echo "检查 $name 服务..."
    
    for i in $(seq 1 $MAX_RETRIES); do
        if curl -sf "${url}${endpoint}" > /dev/null 2>&1; then
            echo "✓ $name 服务可用"
            return 0
        fi
        echo "  尝试 $i/$MAX_RETRIES: $name 未就绪，等待 ${RETRY_INTERVAL}s..."
        sleep $RETRY_INTERVAL
    done
    
    echo "✗ $name 服务不可用"
    return 1
}

test_backend_api() {
    echo ""
    echo "测试后端 API..."
    
    local endpoints=(
        "/health"
        "/docs"
        "/datasources/"
        "/tasks/"
    )
    
    for endpoint in "${endpoints[@]}"; do
        local response=$(curl -sf -o /dev/null -w "%{http_code}" "${BACKEND_URL}${endpoint}")
        if [ "$response" -ge 200 ] && [ "$response" -lt 500 ]; then
            echo "  ✓ $endpoint (HTTP $response)"
        else
            echo "  ✗ $endpoint (HTTP $response)"
        fi
    done
}

test_frontend_static() {
    echo ""
    echo "测试前端静态文件..."
    
    local static_files=(
        "/"
        "/index.html"
    )
    
    for file in "${static_files[@]}"; do
        local response=$(curl -sf -o /dev/null -w "%{http_code}" "${FRONTEND_URL}${file}")
        if [ "$response" = "200" ]; then
            echo "  ✓ $file (HTTP $response)"
        else
            echo "  ✗ $file (HTTP $response)"
        fi
    done
}

test_proxy() {
    echo ""
    echo "测试前端代理到后端..."
    
    local response=$(curl -sf -o /dev/null -w "%{http_code}" "${FRONTEND_URL}/api/datasources/")
    
    if [ "$response" -ge 200 ] && [ "$response" -lt 500 ]; then
        echo "  ✓ /api/datasources/ 代理成功 (HTTP $response)"
    else
        echo "  ✗ /api/datasources/ 代理失败 (HTTP $response)"
        return 1
    fi
}

test_performance() {
    echo ""
    echo "性能测试..."
    
    echo "  测试后端响应时间..."
    local start_time=$(date +%s%3N)
    curl -sf "${BACKEND_URL}/health" > /dev/null
    local end_time=$(date +%s%3N)
    local backend_time=$((end_time - start_time))
    echo "    后端响应时间: ${backend_time}ms"
    
    echo "  测试前端响应时间..."
    local start_time=$(date +%s%3N)
    curl -sf "${FRONTEND_URL}/" > /dev/null
    local end_time=$(date +%s%3N)
    local frontend_time=$((end_time - start_time))
    echo "    前端响应时间: ${frontend_time}ms"
    
    echo "  测试代理响应时间..."
    local start_time=$(date +%s%3N)
    curl -sf "${FRONTEND_URL}/api/datasources/" > /dev/null
    local end_time=$(date +%s%3N)
    local proxy_time=$((end_time - start_time))
    echo "    代理响应时间: ${proxy_time}ms"
}

echo ""
echo "步骤 1: 检查后端服务..."
check_service "Backend" "$BACKEND_URL" "/health"

echo ""
echo "步骤 2: 检查前端服务..."
check_service "Frontend" "$FRONTEND_URL" "/"

echo ""
echo "步骤 3: 运行功能测试..."
test_backend_api
test_frontend_static
test_proxy

echo ""
echo "步骤 4: 运行性能测试..."
test_performance

echo ""
echo "========================================="
echo "测试完成!"
echo "========================================="
echo ""
echo "服务地址:"
echo "  后端 API: $BACKEND_URL"
echo "  前端页面: $FRONTEND_URL"
echo "  API 文档: ${BACKEND_URL}/docs"
echo ""
