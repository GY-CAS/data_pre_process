import sqlite3

conn = sqlite3.connect('database.db')
cursor = conn.cursor()

cursor.execute("PRAGMA table_info(syncedtable)")
columns = cursor.fetchall()

print("SyncedTable table structure:")
for col in columns:
    print(f"  {col[1]} ({col[2]})")

conn.close()
