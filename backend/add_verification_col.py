from sqlmodel import create_engine, text, Session
from backend.app.core.config import settings

def migrate():
    url = settings.get_database_url()
    print(f"Connecting to {url}")
    engine = create_engine(url)
    
    with Session(engine) as session:
        try:
            # Check if column exists
            session.exec(text("SELECT verification_status FROM datatask LIMIT 1"))
            print("Column 'verification_status' already exists.")
        except Exception:
            print("Column 'verification_status' missing. Adding it...")
            try:
                # Add column. MySQL/SQLite syntax compatible enough for simple add
                session.exec(text("ALTER TABLE datatask ADD COLUMN verification_status VARCHAR(50) DEFAULT NULL"))
                session.commit()
                print("Added 'verification_status' column.")
            except Exception as e:
                print(f"Failed to add column: {e}")

if __name__ == "__main__":
    migrate()
