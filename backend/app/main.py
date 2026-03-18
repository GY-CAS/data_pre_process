from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import datasource, task, audit, data_management
from app.core.db import init_database, check_database_health
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Data Preprocessing System API")

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

@app.on_event("startup")
def on_startup():
    logger.info("Starting application...")
    try:
        init_database()
        logger.info("Database initialization completed successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

@app.get("/")
def read_root():
    return {"message": "Welcome to Data Preprocessing System API"}

@app.get("/health")
def health_check():
    db_health = check_database_health()
    return {
        "status": "ok" if db_health["status"] == "healthy" else "degraded",
        "database": db_health
    }
