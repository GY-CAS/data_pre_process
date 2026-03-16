import time
import logging
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, dispatch_function
from starlette.types import ASGIApp

logger = logging.getLogger("api_logger")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    '%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
logger.addHandler(handler)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp):
        super().__init__(app)

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
