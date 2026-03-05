import sys
from sqlmodel import SQLModel, create_engine, Session, select

sys.path.insert(0, '.')

from app.models.datasource import DataSource
from app.models.task import DataTask
from app.models.audit import AuditLog
from app.models.synced_table import SyncedTable

engine = create_engine("sqlite:///database.db")

def update_db_schema():
    print("Updating database schema...")
    
    with Session(engine) as session:
        try:
            existing_data = []
            try:
                existing_data = session.exec(select(SyncedTable)).all()
                print(f"Found {len(existing_data)} existing synced table records")
            except Exception as e:
                print(f"Error reading existing data: {e}")
            
            session.rollback()
        except Exception as e:
            print(f"Error in session: {e}")
    
    try:
        SQLModel.metadata.drop_all(engine)
        print("Dropped existing tables")
    except Exception as e:
        print(f"Error dropping tables: {e}")
    
    SQLModel.metadata.create_all(engine)
    print("Created new tables with updated schema")
    
    with Session(engine) as session:
        for table in existing_data:
            try:
                new_table = SyncedTable(
                    table_name=table.table_name,
                    source_type=table.source_type,
                    source_name=table.source_name,
                    row_count=table.row_count,
                    data_type=None
                )
                session.add(new_table)
            except Exception as e:
                print(f"Error migrating table {table.table_name}: {e}")
        
        try:
            session.commit()
            print("Migrated existing data successfully")
        except Exception as e:
            print(f"Error committing migration: {e}")
            session.rollback()
    
    print("Database schema update completed!")

if __name__ == "__main__":
    update_db_schema()
