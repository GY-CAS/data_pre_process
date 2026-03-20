$ErrorActionPreference = "Continue"

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Docker Compose Container Test Script" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan

$BACKEND_URL = if ($env:BACKEND_URL) { $env:BACKEND_URL } else { "http://localhost:8005" }
$FRONTEND_URL = if ($env:FRONTEND_URL) { $env:FRONTEND_URL } else { "http://localhost:13005" }
$MAX_RETRIES = 10
$RETRY_INTERVAL = 3

function Check-Service {
    param(
        [string]$Name,
        [string]$Url,
        [string]$Endpoint
    )
    
    Write-Host "Checking $Name service..." -ForegroundColor Yellow
    
    for ($i = 1; $i -le $MAX_RETRIES; $i++) {
        try {
            $response = Invoke-WebRequest -Uri "$Url$Endpoint" -UseBasicParsing -TimeoutSec 5 -ErrorAction SilentlyContinue
            if ($response.StatusCode -eq 200) {
                Write-Host "  [OK] $Name service is available" -ForegroundColor Green
                return $true
            }
        } catch {
            # continue waiting
        }
        Write-Host "  Attempt $i/$MAX_RETRIES: $Name not ready, waiting ${RETRY_INTERVAL}s..." -ForegroundColor Gray
        Start-Sleep -Seconds $RETRY_INTERVAL
    }
    
    Write-Host "  [FAIL] $Name service is not available" -ForegroundColor Red
    return $false
}

function Test-BackendApi {
    Write-Host ""
    Write-Host "Testing backend API..." -ForegroundColor Yellow
    
    $endpoints = @(
        "/health",
        "/docs",
        "/datasources/",
        "/tasks/"
    )
    
    foreach ($endpoint in $endpoints) {
        try {
            $statusCode = (Invoke-WebRequest -Uri "$BACKEND_URL$endpoint" -UseBasicParsing -TimeoutSec 10 -ErrorAction SilentlyContinue).StatusCode
            if ($statusCode -ge 200 -and $statusCode -lt 500) {
                Write-Host "  [OK] $endpoint (HTTP $statusCode)" -ForegroundColor Green
            } else {
                Write-Host "  [FAIL] $endpoint (HTTP $statusCode)" -ForegroundColor Red
            }
        } catch {
            Write-Host "  [FAIL] $endpoint (Error: $($_.Exception.Message))" -ForegroundColor Red
        }
    }
}

function Test-FrontendStatic {
    Write-Host ""
    Write-Host "Testing frontend static files..." -ForegroundColor Yellow
    
    $staticFiles = @(
        "/",
        "/index.html"
    )
    
    foreach ($file in $staticFiles) {
        try {
            $statusCode = (Invoke-WebRequest -Uri "$FRONTEND_URL$file" -UseBasicParsing -TimeoutSec 10 -ErrorAction SilentlyContinue).StatusCode
            if ($statusCode -eq 200) {
                Write-Host "  [OK] $file (HTTP $statusCode)" -ForegroundColor Green
            } else {
                Write-Host "  [FAIL] $file (HTTP $statusCode)" -ForegroundColor Red
            }
        } catch {
            Write-Host "  [FAIL] $file (Error: $($_.Exception.Message))" -ForegroundColor Red
        }
    }
}

function Test-Proxy {
    Write-Host ""
    Write-Host "Testing frontend proxy to backend..." -ForegroundColor Yellow
    
    try {
        $statusCode = (Invoke-WebRequest -Uri "$FRONTEND_URL/api/datasources/" -UseBasicParsing -TimeoutSec 10 -ErrorAction SilentlyContinue).StatusCode
        if ($statusCode -ge 200 -and $statusCode -lt 500) {
            Write-Host "  [OK] /api/datasources/ proxy success (HTTP $statusCode)" -ForegroundColor Green
        } else {
            Write-Host "  [FAIL] /api/datasources/ proxy failed (HTTP $statusCode)" -ForegroundColor Red
        }
    } catch {
        Write-Host "  [FAIL] /api/datasources/ proxy failed (Error: $($_.Exception.Message))" -ForegroundColor Red
    }
}

function Test-Performance {
    Write-Host ""
    Write-Host "Performance test..." -ForegroundColor Yellow
    
    # Test backend response time
    Write-Host "  Testing backend response time..." -ForegroundColor Gray
    $sw = [Diagnostics.Stopwatch]::StartNew()
    try {
        $null = Invoke-WebRequest -Uri "$BACKEND_URL/health" -UseBasicParsing -TimeoutSec 10 -ErrorAction SilentlyContinue
        $sw.Stop()
        Write-Host "    Backend response time: $($sw.ElapsedMilliseconds)ms" -ForegroundColor Cyan
    } catch {
        $sw.Stop()
        Write-Host "    Backend response failed" -ForegroundColor Red
    }
    
    # Test frontend response time
    Write-Host "  Testing frontend response time..." -ForegroundColor Gray
    $sw = [Diagnostics.Stopwatch]::StartNew()
    try {
        $null = Invoke-WebRequest -Uri "$FRONTEND_URL/" -UseBasicParsing -TimeoutSec 10 -ErrorAction SilentlyContinue
        $sw.Stop()
        Write-Host "    Frontend response time: $($sw.ElapsedMilliseconds)ms" -ForegroundColor Cyan
    } catch {
        $sw.Stop()
        Write-Host "    Frontend response failed" -ForegroundColor Red
    }
    
    # Test proxy response time
    Write-Host "  Testing proxy response time..." -ForegroundColor Gray
    $sw = [Diagnostics.Stopwatch]::StartNew()
    try {
        $null = Invoke-WebRequest -Uri "$FRONTEND_URL/api/datasources/" -UseBasicParsing -TimeoutSec 10 -ErrorAction SilentlyContinue
        $sw.Stop()
        Write-Host "    Proxy response time: $($sw.ElapsedMilliseconds)ms" -ForegroundColor Cyan
    } catch {
        $sw.Stop()
        Write-Host "    Proxy response failed" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "Step 1: Checking backend service..." -ForegroundColor Yellow
$backendOk = Check-Service -Name "Backend" -Url $BACKEND_URL -Endpoint "/health"

Write-Host ""
Write-Host "Step 2: Checking frontend service..." -ForegroundColor Yellow
$frontendOk = Check-Service -Name "Frontend" -Url $FRONTEND_URL -Endpoint "/"

if ($backendOk -and $frontendOk) {
    Write-Host ""
    Write-Host "Step 3: Running functional tests..." -ForegroundColor Yellow
    Test-BackendApi
    Test-FrontendStatic
    Test-Proxy
    
    Write-Host ""
    Write-Host "Step 4: Running performance tests..." -ForegroundColor Yellow
    Test-Performance
} else {
    Write-Host ""
    Write-Host "Skipping functional tests because services are not ready" -ForegroundColor Red
}

Write-Host ""
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Test Completed!" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Service addresses:" -ForegroundColor White
Write-Host "  Backend API: $BACKEND_URL" -ForegroundColor White
Write-Host "  Frontend: $FRONTEND_URL" -ForegroundColor White
Write-Host "  API Docs: $BACKEND_URL/docs" -ForegroundColor White
Write-Host ""
