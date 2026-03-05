import sqlite3

# Connect to the database
conn = sqlite3.connect('database.db')
cursor = conn.cursor()

# Get all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables in database:")
for table in tables:
    print(f"  - {table[0]}")

# Check syncedtable content
print("\nSyncedTable content:")
cursor.execute("SELECT * FROM syncedtable LIMIT 10;")
synced_tables = cursor.fetchall()
if synced_tables:
    print("ID, Table Name, Source Type, Source Name, Row Count, Data Type, Created At, Updated At")
    for row in synced_tables:
        print(row)
else:
    print("No synced tables found")

# Check if there are any data tables
print("\nChecking for data tables:")
data_tables = [table[0] for table in tables if table[0] not in ['datasource', 'datatask', 'auditlog', 'syncedtable']]
if data_tables:
    print("Data tables found:")
    for table in data_tables:
        print(f"  - {table}")
        # Check table structure
        cursor.execute(f"PRAGMA table_info({table});")
        columns = cursor.fetchall()
        print("    Columns:")
        for col in columns:
            print(f"      {col[1]} ({col[2]})")
        # Check row count
        cursor.execute(f"SELECT count(*) FROM {table};")
        count = cursor.fetchone()[0]
        print(f"    Row count: {count}")
else:
    print("No data tables found")

conn.close()
