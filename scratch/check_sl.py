import sqlite3
import os

DB_FILE = 'trade.db'
conn = sqlite3.connect(DB_FILE)
conn.row_factory = sqlite3.Row
cursor = conn.row_factory = conn.cursor()
cursor.execute("SELECT * FROM my_stocks WHERE code='425040'")
row = cursor.fetchone()
if row:
    print(dict(zip([column[0] for column in cursor.description], row)))
conn.close()
