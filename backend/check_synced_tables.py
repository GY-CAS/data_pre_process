from sqlmodel import Session, select
from app.core.db import engine
from app.models.synced_table import SyncedTable

# 查询数据库中的SyncedTable记录
with Session(engine) as session:
    synced_tables = session.exec(select(SyncedTable)).all()
    print(f"Total synced tables: {len(synced_tables)}")
    print("\nSynced tables:")
    for table in synced_tables:
        print(f"- ID: {table.id}")
        print(f"  Table name: {table.table_name}")
        print(f"  Source type: {table.source_type}")
        print(f"  Source name: {table.source_name}")
        print(f"  Row count: {table.row_count}")
        print(f"  Data type: {table.data_type}")
        print(f"  Created at: {table.created_at}")
        print(f"  Updated at: {table.updated_at}")
        print()
