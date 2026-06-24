# -*- coding: utf-8 -*-
import os
import sys
import sqlite3
import pymysql
import pymysql.cursors
from urllib.parse import urlparse
from dotenv import load_dotenv

# Ensure UTF-8 console output
if os.name == 'nt':
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Load env variables from .env
base_dir = os.path.dirname(__file__)
load_dotenv(os.path.join(base_dir, '.env'))

DATABASE_URL = os.getenv('DATABASE_URL')
SQLITE_PATH = os.path.join(base_dir, 'trade.db')

# Add current dir to path to import db_init
sys.path.append(base_dir)
from db_init import _init_sqlite

def main():
    print("==================================================")
    print("  MySQL -> SQLite Backup & Migration Script")
    print("==================================================")

    if not DATABASE_URL or not DATABASE_URL.startswith('mysql'):
        print("[ERROR] DATABASE_URL is not configured for MySQL.")
        sys.exit(1)

    print(f"[*] Target SQLite File: {SQLITE_PATH}")
    print("[*] Initializing SQLite database schema...")
    
    # Initialize SQLite DB schema using db_init function
    if os.path.exists(SQLITE_PATH):
        print("[*] Existing trade.db file found. Removing for a clean migration...")
        try:
            os.remove(SQLITE_PATH)
        except Exception as e:
            print(f"[WARNING] Failed to remove existing trade.db: {e}")
            
    _init_sqlite()
    print("[+] SQLite schema initialized successfully.")

    # Connect to MySQL
    print("[*] Connecting to MySQL database...")
    parsed = urlparse(DATABASE_URL)
    db_opts = {
        'host': parsed.hostname or '127.0.0.1',
        'port': parsed.port or 3306,
        'user': parsed.username or 'root',
        'password': parsed.password or '',
        'database': parsed.path.lstrip('/') if parsed.path else 'trade',
        'charset': 'utf8mb4',
    }
    
    try:
        mysql_conn = pymysql.connect(**db_opts, cursorclass=pymysql.cursors.DictCursor)
    except Exception as e:
        print(f"[ERROR] Failed to connect to MySQL: {e}")
        sys.exit(1)
    
    # Connect to SQLite
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    
    tables = [
        'tr_my_stocks',
        'tr_stock_daily_history',
        'tr_sell_history',
        'tr_stocks_master',
        'tr_portfolio_ai_cache',
        'tr_stock_pool',
        'tr_audit_recommendations'
    ]
    
    total_migrated = 0
    try:
        for table in tables:
            print(f"[*] Migrating table: {table}...")
            mysql_cur = mysql_conn.cursor()
            
            # Check if table exists in MySQL first
            try:
                mysql_cur.execute(f"SELECT * FROM {table}")
                rows = mysql_cur.fetchall()
            except Exception as e:
                print(f"[WARNING] Table {table} does not exist in MySQL or error occurred: {e}")
                continue
                
            if not rows:
                print(f"[-] {table}: No records found in MySQL.")
                continue
                
            # SQLite insertion cursor
            sqlite_cur = sqlite_conn.cursor()
            
            # Extract columns from the first row keys
            columns = list(rows[0].keys())
            col_placeholders = ", ".join(["?"] * len(columns))
            col_names = ", ".join(columns)
            
            insert_query = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({col_placeholders})"
            
            # Handle decimal values and formatting
            from decimal import Decimal
            data_to_insert = []
            for r in rows:
                row_vals = []
                for col in columns:
                    val = r[col]
                    if isinstance(val, Decimal):
                        val = float(val)
                    row_vals.append(val)
                data_to_insert.append(row_vals)
                
            sqlite_cur.executemany(insert_query, data_to_insert)
            sqlite_conn.commit()
            print(f"[+] {table}: Successfully migrated {len(rows)} records.")
            total_migrated += len(rows)
            
        print("==================================================")
        print(f"[SUCCESS] Migration completed! Total {total_migrated} records written to SQLite.")
        print("==================================================")
        
    except Exception as e:
        print(f"[ERROR] Migration failed: {e}")
        sqlite_conn.rollback()
    finally:
        mysql_conn.close()
        sqlite_conn.close()

if __name__ == '__main__':
    main()
