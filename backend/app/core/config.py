
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

    def get_database_url(self) -> str:
        # Use MySQL if configured, otherwise fallback to SQLite
        if self.MYSQL_HOST and self.MYSQL_USER:
            return f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DB}"
        return "sqlite:///database.db"

    class Config:
        env_file = ".env"
        extra = "ignore" # Ignore extra fields in .env

settings = Settings()
