import os
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

def find_env_file() -> Path:
    current = Path(__file__).resolve()
    env_files = [".env.docker", ".env"]
    for parent in [current.parent] + list(current.parents):
        for env_file in env_files:
            env_path = parent / env_file
            if env_path.exists():
                return env_path
    return Path(".env")

ENV_FILE_PATH = find_env_file()

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE_PATH), 
        extra="ignore", 
        case_sensitive=False
    )
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Data Preprocessing System"
    
    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "root"
    MYSQL_PASSWORD: str = ""
    SYSTEM_DB_NAME: str = "data_preprocess"
    
    MYSQL_DB: str = "test_db"

    @property
    def SYSTEM_DB_URL(self) -> str:
        return self.get_database_url()

    def get_database_url(self) -> str:
        from urllib.parse import quote_plus
        encoded_password = quote_plus(self.MYSQL_PASSWORD)
        return f"mysql+pymysql://{self.MYSQL_USER}:{encoded_password}@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.SYSTEM_DB_NAME}?charset=utf8mb4"

    CK_HOST: str = "localhost"
    CK_PORT: int = 9002
    CK_USER: str = "default"
    CK_PASSWORD: str = "default"

    MINIO_ENDPOINT: str = "http://localhost:9000"
    MINIO_ROOT_USER: str = "minioadmin"
    MINIO_ROOT_PASSWORD: str = "minioadmin"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"

settings = Settings()
