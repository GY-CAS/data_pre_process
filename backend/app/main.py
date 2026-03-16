import time
import logging
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from app.api import datasource, task, audit, data_management

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("api")

class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        request_body = None
        if request.method in ["POST", "PUT", "PATCH"]:
            try:
                body = await request.body()
                request_body = body.decode('utf-8')[:500]
            except:
                request_body = "<binary data>"

        response: Response = await call_next(request)
        
        process_time = (time.time() - start_time) * 1000
        
        logger.info(
            f"{request.method} {request.url.path} | "
            f"Status: {response.status_code} | "
            f"Time: {process_time:.2f}ms | "
            f"IP: {request.client.host if request.client else 'unknown'} | "
            f"Query: {str(request.query_params)[:100] if request.query_params else '-'} | "
            f"Body: {request_body[:100] if request_body else '-'}"
        )
        
        return response

app = FastAPI(
    title="Data Preprocessing System API",
    description="""
## 数据预处理系统 API

提供数据源管理、任务管理、审计日志、数据资产管理等功能。

### 功能模块

- **数据源管理 (DataSources)**: 管理 MySQL、ClickHouse、MinIO 等数据源连接
- **任务管理 (Tasks)**: 创建、执行、监控数据处理任务
- **审计日志 (Audit)**: 记录系统操作日志
- **数据管理 (Data Management)**: 数据资产预览、导出、编辑

### 认证

当前版本无需认证，生产环境请配置相应的认证机制。
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(RequestLoggingMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(datasource.router)
app.include_router(task.router)
app.include_router(audit.router)
app.include_router(data_management.router)

@app.get("/", tags=["root"], summary="API根路径", description="返回API欢迎信息")
def read_root():
    """
    API根路径端点
    
    返回欢迎消息，用于验证API服务是否正常运行。
    """
    return {"message": "Welcome to Data Preprocessing System API"}

@app.get("/health", tags=["root"], summary="健康检查", description="检查服务健康状态")
def health_check():
    """
    健康检查端点
    
    用于容器编排和负载均衡器的健康检查。
    """
    return {"status": "healthy", "service": "data-preprocessing-api"}

@app.get("/health/db", tags=["root"], summary="数据库连接检查", description="检查所有数据库连接状态")
def check_database_connections():
    """
    数据库连接健康检查端点
    
    检查 MySQL、ClickHouse、MinIO 的连接状态。
    """
    from app.core.config import settings
    import socket
    
    results = {
        "mysql": {"status": "unknown", "host": settings.MYSQL_HOST, "port": settings.MYSQL_PORT},
        "clickhouse": {"status": "unknown", "host": settings.CK_HOST, "port": settings.CK_PORT},
        "minio": {"status": "unknown", "endpoint": settings.MINIO_ENDPOINT}
    }
    
    # Check MySQL
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((settings.MYSQL_HOST, settings.MYSQL_PORT))
        sock.close()
        results["mysql"]["status"] = "reachable" if result == 0 else "unreachable"
    except Exception as e:
        results["mysql"]["status"] = f"error: {str(e)}"
    
    # Check ClickHouse
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((settings.CK_HOST, settings.CK_PORT))
        sock.close()
        results["clickhouse"]["status"] = "reachable" if result == 0 else "unreachable"
    except Exception as e:
        results["clickhouse"]["status"] = f"error: {str(e)}"
    
    # Check MinIO
    try:
        import urllib.parse
        parsed = urllib.parse.urlparse(settings.MINIO_ENDPOINT)
        host = parsed.netloc.split(':')[0]
        port = int(parsed.netloc.split(':')[1]) if ':' in parsed.netloc else 9000
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()
        results["minio"]["status"] = "reachable" if result == 0 else "unreachable"
        results["minio"]["host"] = host
        results["minio"]["port"] = port
    except Exception as e:
        results["minio"]["status"] = f"error: {str(e)}"
    
    all_healthy = all(
        v["status"] == "reachable" or v["status"] == "unknown" 
        for v in results.values()
    )
    
    return {
        "status": "healthy" if all_healthy else "degraded",
        "connections": results,
        "config": {
            "mysql_host": settings.MYSQL_HOST,
            "mysql_port": settings.MYSQL_PORT,
            "ck_host": settings.CK_HOST,
            "ck_port": settings.CK_PORT,
            "minio_endpoint": settings.MINIO_ENDPOINT
        }
    }
