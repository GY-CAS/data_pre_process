
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # DATABASE_URL: str = "sqlite:///database.db" # Removed to use property
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Data Preprocessing System"
    
    # System Database for Sync Targets
    @property
    def SYSTEM_DB_URL(self) -> str:
        # Use same MySQL DB as main app for simplicity, or could use a separate one
        return self.get_database_url()

    # MySQL Test Config
    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "root"
    MYSQL_PASSWORD: str = ""
    MYSQL_DB: str = "test_db"

    # ClickHouse Configuration
    CK_HOST: str = "localhost"
    CK_PORT: int = 9000
    CK_USER: str = "default"
    CK_PASSWORD: str = ""

    # MinIO Configuration
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ROOT_USER: str = "minioadmin"
    MINIO_ROOT_PASSWORD: str = "minioadmin"
    
    # CK_DB is not in env, defaulting to 'default' or handled dynamically?
    # User env has CK_host, CK_port, CK_user, CK_password.
    # Note: env file has lowercase keys CK_host, etc. Pydantic reads case-insensitive if configured, 
    # but environment variables are usually case-sensitive on Linux. 
    # .env file content: CK_host='localhost' ...
    # BaseSettings reads from .env.

    def get_database_url(self) -> str:
        # Use MySQL if configured, otherwise fallback to SQLite
        if self.MYSQL_HOST and self.MYSQL_USER:
            return f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DB}"
        return "sqlite:///database.db"
    
    class Config:
        env_file = ".env"
        extra = "ignore" # Ignore extra fields in .env

settings = Settings()
