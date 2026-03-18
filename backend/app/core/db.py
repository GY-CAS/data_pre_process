from sqlmodel import SQLModel, create_engine, Session, text
from app.core.config import settings
from sqlalchemy.exc import OperationalError, ProgrammingError
from urllib.parse import quote_plus
import logging

logger = logging.getLogger(__name__)

def get_root_database_url() -> str:
    encoded_password = quote_plus(settings.MYSQL_PASSWORD)
    return f"mysql+pymysql://{settings.MYSQL_USER}:{encoded_password}@{settings.MYSQL_HOST}:{settings.MYSQL_PORT}/mysql"

def get_database_url() -> str:
    encoded_password = quote_plus(settings.MYSQL_PASSWORD)
    return f"mysql+pymysql://{settings.MYSQL_USER}:{encoded_password}@{settings.MYSQL_HOST}:{settings.MYSQL_PORT}/{settings.SYSTEM_DB_NAME}?charset=utf8mb4"

database_url = get_database_url()

def test_mysql_connection() -> bool:
    try:
        import pymysql
        conn = pymysql.connect(
            host=settings.MYSQL_HOST,
            port=settings.MYSQL_PORT,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            charset='utf8mb4',
            connect_timeout=5
        )
        print(f"[DB Test] MySQL connection successful: {settings.MYSQL_HOST}:{settings.MYSQL_PORT}")
        conn.close()
        return True
    except Exception as e:
        print(f"[DB Test] MySQL connection failed: {e}")
        return False

def create_db_if_not_exists():
    try:
        if not test_mysql_connection():
            raise Exception("MySQL server is not reachable")
        
        root_url = get_root_database_url()
        tmp_engine = create_engine(
            root_url, 
            echo=False, 
            pool_pre_ping=True,
            connect_args={'connect_timeout': 10}
        )
        with tmp_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{settings.SYSTEM_DB_NAME}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
            conn.commit()
            print(f"[DB Init] Database '{settings.SYSTEM_DB_NAME}' ensured.")
        tmp_engine.dispose()
    except Exception as e:
        print(f"[DB Init] Error creating database: {e}")
        raise

engine = None

def get_engine():
    global engine
    if engine is None:
        create_db_if_not_exists()
        engine = create_engine(
            database_url, 
            echo=False,
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            connect_args={
                'connect_timeout': 10,
                'charset': 'utf8mb4'
            }
        )
    return engine

def create_db_and_tables():
    create_db_if_not_exists()
    current_engine = get_engine()
    SQLModel.metadata.create_all(current_engine)
    print("[DB Init] All tables ensured.")

def get_session():
    current_engine = get_engine()
    with Session(current_engine) as session:
        yield session

def init_database():
    print(f"[DB Init] Initializing database '{settings.SYSTEM_DB_NAME}'...")
    print(f"[DB Init] Connecting to {settings.MYSQL_HOST}:{settings.MYSQL_PORT}")
    create_db_and_tables()
    print("[DB Init] Database initialization completed.")

def check_database_health() -> dict:
    try:
        current_engine = get_engine()
        with current_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "database": settings.SYSTEM_DB_NAME, "host": settings.MYSQL_HOST}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
