from sqlmodel import create_engine, text, Session
from backend.app.core.config import settings

def migrate():
    url = settings.get_database_url()
    print(f"Connecting to {url}")
    engine = create_engine(url)
    
    with Session(engine) as session:
        try:
            # Check if column exists
            session.exec(text("SELECT progress FROM datatask LIMIT 1"))
            print("Column 'progress' already exists.")
        except Exception:
            print("Column 'progress' missing. Adding it...")
            try:
                session.exec(text("ALTER TABLE datatask ADD COLUMN progress INTEGER DEFAULT 0"))
                session.commit()
                print("Added 'progress' column.")
            except Exception as e:
                print(f"Failed to add column: {e}")

if __name__ == "__main__":
    migrate()
