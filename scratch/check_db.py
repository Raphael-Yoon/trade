import sqlite3
import os

DB_FILE = 'trade.db'
if os.path.exists(DB_FILE):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM price_alerts WHERE type='after_hours'")
    print(f"After-hours alerts count: {cursor.fetchone()[0]}")
    conn.close()
else:
    print("DB file not found")
