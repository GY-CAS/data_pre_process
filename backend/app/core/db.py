from sqlmodel import SQLModel, create_engine, Session, text
from backend.app.core.config import settings
from sqlalchemy.exc import OperationalError

connect_args = {}
database_url = settings.get_database_url()
if database_url.startswith("sqlite"):
    connect_args["check_same_thread"] = False

def create_db_if_not_exists():
    if database_url.startswith("mysql"):
        try:
            # Create a temporary engine without the database name to check/create DB
            # Parse the URL to remove the DB name for the initial connection
            # Assuming URL format: mysql+pymysql://user:pass@host:port/dbname
            root_url = database_url.rsplit("/", 1)[0] 
            # We need to connect to a default DB, usually 'mysql' or just root. 
            # SQLAlchemy might need a DB name, so let's try connecting to 'mysql' or just root if driver allows.
            # PyMySQL allows no DB.
            # But create_engine needs a valid URL. Let's try connecting to 'information_schema' or 'mysql'
            # Or just strip the DB name if using pymysql.
            
            # Safer approach: Parse components
            from sqlalchemy.engine.url import make_url
            url = make_url(database_url)
            db_name = url.database
            
            # Create engine for 'mysql' database to execute CREATE DATABASE
            # Copy URL and set database to 'mysql' (which always exists) or None
            root_url = url._replace(database='mysql')
            
            tmp_engine = create_engine(root_url, echo=True)
            with tmp_engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
                print(f"Database {db_name} ensured.")
        except Exception as e:
            print(f"Warning: Could not check/create database: {e}")

# Call this before creating the main engine? No, main engine is global. 
# But we can run it before create_db_and_tables.

engine = create_engine(database_url, echo=True, connect_args=connect_args)

def create_db_and_tables():
    create_db_if_not_exists()
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session
