# -*- coding: utf-8 -*-
import os
import sqlite3
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
DATABASE_URL = os.getenv('DATABASE_URL')

SQLITE_PATH = os.path.join(os.path.dirname(__file__), 'trade.db')


def init_db():
    if DATABASE_URL and DATABASE_URL.startswith('mysql'):
        _init_mysql()
    else:
        _init_sqlite()


def _init_sqlite():
    conn = sqlite3.connect(SQLITE_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_my_stocks (
            code TEXT PRIMARY KEY,
            name TEXT,
            added_at TEXT,
            purchase_price REAL DEFAULT 0,
            quantity INTEGER DEFAULT 0,
            stop_loss_ratio REAL DEFAULT 0,
            is_favorite INTEGER DEFAULT 0,
            peak_price REAL DEFAULT 0,
            owner TEXT DEFAULT '나',
            type TEXT DEFAULT 'portfolio',
            target_buy_price REAL DEFAULT 0
        )
    ''')
    for col_name, col_def in [
        ('purchase_price', 'REAL DEFAULT 0'),
        ('quantity', 'INTEGER DEFAULT 0'),
        ('stop_loss_ratio', 'REAL DEFAULT 0'),
        ('is_favorite', 'INTEGER DEFAULT 0'),
        ('peak_price', 'REAL DEFAULT 0'),
        ('owner', "TEXT DEFAULT '나'"),
        ('type', "TEXT DEFAULT 'portfolio'"),
        ('target_buy_price', 'REAL DEFAULT 0'),
    ]:
        try:
            cursor.execute(f"ALTER TABLE tr_my_stocks ADD COLUMN {col_name} {col_def}")
        except Exception:
            pass

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_stock_daily_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            code TEXT,
            name TEXT,
            purchase_price REAL,
            current_price REAL,
            quantity INTEGER,
            owner TEXT,
            recorded_at TEXT,
            day_profit REAL DEFAULT 0,
            cumulative_profit REAL DEFAULT 0,
            change_rate REAL DEFAULT 0
        )
    ''')
    for col_name, col_def in [
        ('day_profit', 'REAL DEFAULT 0'),
        ('cumulative_profit', 'REAL DEFAULT 0'),
        ('change_rate', 'REAL DEFAULT 0'),
    ]:
        try:
            cursor.execute(f"ALTER TABLE tr_stock_daily_history ADD COLUMN {col_name} {col_def}")
        except Exception:
            pass

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_sell_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT,
            name TEXT,
            owner TEXT,
            sell_price REAL,
            sell_qty INTEGER,
            purchase_price REAL,
            profit REAL,
            profit_rate REAL,
            sell_date TEXT,
            created_at TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_stocks_master (
            code TEXT PRIMARY KEY,
            name TEXT,
            market TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_portfolio_ai_cache (
            cache_key TEXT PRIMARY KEY,
            ai_result TEXT,
            created_at TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_stock_pool (
            code TEXT,
            name TEXT,
            sector TEXT,
            roe REAL,
            pbr REAL,
            per REAL,
            debt_ratio REAL,
            operating_margin REAL,
            target_price REAL,
            foreign_net_buy REAL,
            inst_net_buy REAL,
            pool_score REAL,
            data_date TEXT,
            updated_at TEXT,
            market_cap REAL,
            is_sector_leader INTEGER,
            source_file TEXT,
            PRIMARY KEY (code, source_file)
        )
    ''')
    for col_name, col_def in [
        ('market_cap', 'REAL'),
        ('is_sector_leader', 'INTEGER'),
        ('source_file', 'TEXT'),
        ('sector_category', 'TEXT'),
    ]:
        try:
            cursor.execute(f"ALTER TABLE tr_stock_pool ADD COLUMN {col_name} {col_def}")
        except Exception:
            pass

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_audit_recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT,
            name TEXT,
            sector TEXT,
            current_price REAL,
            target_price REAL,
            upside REAL,
            opinion TEXT,
            data_date TEXT,
            created_at TEXT,
            score REAL DEFAULT 0.0,
            roe REAL DEFAULT 0.0,
            debt REAL DEFAULT 0.0,
            reason TEXT,
            news_summary TEXT,
            rec_type TEXT DEFAULT 'momentum',
            one_liner TEXT DEFAULT '',
            disc_json TEXT
        )
    ''')
    for col_name, col_def in [
        ('rec_type', "TEXT DEFAULT 'momentum'"),
        ('one_liner', "TEXT DEFAULT ''"),
        ('disc_json', 'TEXT'),
        ('sector', 'TEXT'),
    ]:
        try:
            cursor.execute(f"ALTER TABLE tr_audit_recommendations ADD COLUMN {col_name} {col_def}")
        except Exception:
            pass

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tr_stock_daily_history_date ON tr_stock_daily_history(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_date_owner ON tr_stock_daily_history(date, owner)")

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_settings (
            "key" TEXT PRIMARY KEY,
            value TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_investment_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            content TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    ''')
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_journal_date ON tr_investment_journal(date)")

    # [김선화/김정음] 해외 주식 관리 테이블 (id 및 증권사 broker 컬럼 지원)
    cursor.execute("PRAGMA table_info(tr_overseas_stocks)")
    existing_cols = [r[1] for r in cursor.fetchall()]
    if existing_cols and 'id' not in existing_cols:
        cursor.execute("ALTER TABLE tr_overseas_stocks RENAME TO tr_overseas_stocks_old")
        cursor.execute('''
            CREATE TABLE tr_overseas_stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                name TEXT,
                broker TEXT DEFAULT '기본',
                quantity REAL DEFAULT 0,
                purchase_price REAL DEFAULT 0,
                close_price REAL DEFAULT 0,
                price_change REAL DEFAULT 0,
                fluctuations_ratio REAL DEFAULT 0,
                dividend_per_share REAL DEFAULT 0,
                dividend_yield REAL DEFAULT 0,
                dividend_date TEXT,
                ex_dividend_date TEXT,
                exchange_rate REAL DEFAULT 0,
                currency TEXT DEFAULT 'USD',
                price_date TEXT,
                updated_at TEXT,
                notes TEXT
            )
        ''')
        try:
            cursor.execute('''
                INSERT INTO tr_overseas_stocks (symbol, name, broker, quantity, purchase_price, close_price, price_change, fluctuations_ratio, dividend_per_share, dividend_yield, dividend_date, ex_dividend_date, exchange_rate, currency, price_date, updated_at, notes)
                SELECT symbol, name, '기본', quantity, purchase_price, close_price, price_change, fluctuations_ratio, dividend_per_share, dividend_yield, dividend_date, ex_dividend_date, exchange_rate, currency, price_date, updated_at, notes FROM tr_overseas_stocks_old
            ''')
        except Exception:
            pass
        cursor.execute("DROP TABLE IF EXISTS tr_overseas_stocks_old")
    else:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tr_overseas_stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                name TEXT,
                broker TEXT DEFAULT '기본',
                quantity REAL DEFAULT 0,
                purchase_price REAL DEFAULT 0,
                close_price REAL DEFAULT 0,
                price_change REAL DEFAULT 0,
                fluctuations_ratio REAL DEFAULT 0,
                dividend_per_share REAL DEFAULT 0,
                dividend_yield REAL DEFAULT 0,
                dividend_date TEXT,
                ex_dividend_date TEXT,
                exchange_rate REAL DEFAULT 0,
                currency TEXT DEFAULT 'USD',
                price_date TEXT,
                updated_at TEXT,
                notes TEXT
            )
        ''')
        try:
            cursor.execute("ALTER TABLE tr_overseas_stocks ADD COLUMN broker TEXT DEFAULT '기본'")
        except Exception:
            pass

    # [김선화/김정음] 배당금 수령 내역 관리 테이블 (broker 컬럼 지원)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_dividend_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            name TEXT,
            broker TEXT DEFAULT '기본',
            market_type TEXT DEFAULT 'OVERSEAS',
            payment_date TEXT,
            amount_gross REAL DEFAULT 0,
            amount_net REAL DEFAULT 0,
            currency TEXT DEFAULT 'USD',
            exchange_rate REAL DEFAULT 0,
            amount_net_krw REAL DEFAULT 0,
            notes TEXT,
            created_at TEXT
        )
    ''')
    try:
        cursor.execute("ALTER TABLE tr_dividend_history ADD COLUMN broker TEXT DEFAULT '기본'")
    except Exception:
        pass
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_dividend_date ON tr_dividend_history(payment_date)")
    # [김선화/김정음] 해외 주식 일별 스냅샷 히스토리 테이블 (SQLite)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_overseas_daily_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            symbol TEXT,
            name TEXT,
            broker TEXT DEFAULT '기본',
            quantity REAL DEFAULT 0,
            purchase_price REAL DEFAULT 0,
            close_price REAL DEFAULT 0,
            price_change REAL DEFAULT 0,
            fluctuations_ratio REAL DEFAULT 0,
            exchange_rate REAL DEFAULT 0,
            currency TEXT DEFAULT 'USD',
            day_profit_usd REAL DEFAULT 0,
            day_profit_krw REAL DEFAULT 0,
            cumulative_profit_usd REAL DEFAULT 0,
            cumulative_profit_krw REAL DEFAULT 0,
            cumulative_profit_rate REAL DEFAULT 0,
            eval_usd REAL DEFAULT 0,
            eval_krw REAL DEFAULT 0,
            cost_usd REAL DEFAULT 0,
            cost_krw REAL DEFAULT 0,
            recorded_at TEXT
        )
    ''')
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ov_daily_date ON tr_overseas_daily_history(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ov_daily_symbol_date ON tr_overseas_daily_history(symbol, date)")

    conn.commit()
    conn.close()


def _init_mysql():
    import pymysql
    from urllib.parse import urlparse

    parsed = urlparse(DATABASE_URL)
    db_opts = {
        'host': parsed.hostname or '127.0.0.1',
        'port': parsed.port or 3306,
        'user': parsed.username or 'root',
        'password': parsed.password or '',
        'database': parsed.path.lstrip('/') if parsed.path else 'trade',
        'charset': 'utf8mb4',
    }
    conn = pymysql.connect(**db_opts)
    conn.autocommit(True)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'tr_stock_pool' AND column_name = 'source_file'
    """)
    if cursor.fetchone()[0] == 0:
        print("[DB 마이그레이션] MySQL tr_stock_pool 테이블에 source_file 컬럼이 없어 기존 테이블을 드롭하고 재생성합니다.")
        cursor.execute("DROP TABLE IF EXISTS tr_stock_pool")

    def add_column_if_not_exists(table, col, definition):
        cursor.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = '{table}' AND column_name = '{col}'
        """)
        if cursor.fetchone()[0] == 0:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} {definition}")

    def create_index_if_not_exists(idx_name, table, col):
        cursor.execute(f"""
            SELECT COUNT(*) FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = '{table}' AND index_name = '{idx_name}'
        """)
        if cursor.fetchone()[0] == 0:
            cursor.execute(f"CREATE INDEX {idx_name} ON {table}({col})")

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_my_stocks (
            code VARCHAR(50) PRIMARY KEY,
            name VARCHAR(255),
            added_at VARCHAR(50),
            purchase_price DOUBLE DEFAULT 0,
            quantity INT DEFAULT 0,
            stop_loss_ratio DOUBLE DEFAULT 0,
            is_favorite INT DEFAULT 0,
            peak_price DOUBLE DEFAULT 0,
            owner VARCHAR(50) DEFAULT '나',
            type VARCHAR(50) DEFAULT 'portfolio',
            target_buy_price DOUBLE DEFAULT 0
        )
    ''')
    for col_name, col_def in [
        ('purchase_price', 'DOUBLE DEFAULT 0'),
        ('quantity', 'INT DEFAULT 0'),
        ('stop_loss_ratio', 'DOUBLE DEFAULT 0'),
        ('is_favorite', 'INT DEFAULT 0'),
        ('peak_price', 'DOUBLE DEFAULT 0'),
        ('owner', "VARCHAR(50) DEFAULT '나'"),
        ('type', "VARCHAR(50) DEFAULT 'portfolio'"),
        ('target_buy_price', 'DOUBLE DEFAULT 0'),
    ]:
        add_column_if_not_exists("tr_my_stocks", col_name, col_def)

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_stock_daily_history (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date VARCHAR(50),
            code VARCHAR(50),
            name VARCHAR(255),
            purchase_price DOUBLE,
            current_price DOUBLE,
            quantity INT,
            owner VARCHAR(50),
            recorded_at VARCHAR(50),
            day_profit DOUBLE DEFAULT 0,
            cumulative_profit DOUBLE DEFAULT 0,
            change_rate DOUBLE DEFAULT 0
        )
    ''')
    for col_name, col_def in [
        ('day_profit', 'DOUBLE DEFAULT 0'),
        ('cumulative_profit', 'DOUBLE DEFAULT 0'),
        ('change_rate', 'DOUBLE DEFAULT 0'),
    ]:
        add_column_if_not_exists("tr_stock_daily_history", col_name, col_def)

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_sell_history (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(50),
            name VARCHAR(255),
            owner VARCHAR(50),
            sell_price DOUBLE,
            sell_qty INT,
            purchase_price DOUBLE,
            profit DOUBLE,
            profit_rate DOUBLE,
            sell_date VARCHAR(50),
            created_at VARCHAR(50)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_stocks_master (
            code VARCHAR(50) PRIMARY KEY,
            name VARCHAR(255),
            market VARCHAR(50)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_portfolio_ai_cache (
            cache_key VARCHAR(255) PRIMARY KEY,
            ai_result LONGTEXT,
            created_at VARCHAR(50)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_stock_pool (
            code VARCHAR(50),
            name VARCHAR(255),
            sector VARCHAR(255),
            roe DOUBLE,
            pbr DOUBLE,
            per DOUBLE,
            debt_ratio DOUBLE,
            operating_margin DOUBLE,
            target_price DOUBLE,
            foreign_net_buy DOUBLE,
            inst_net_buy DOUBLE,
            pool_score DOUBLE,
            data_date VARCHAR(50),
            updated_at VARCHAR(50),
            market_cap DOUBLE,
            is_sector_leader BOOLEAN,
            source_file VARCHAR(255),
            PRIMARY KEY (code, source_file)
        )
    ''')
    for col_name, col_def in [
        ('market_cap', 'DOUBLE'),
        ('is_sector_leader', 'BOOLEAN'),
        ('source_file', 'VARCHAR(255)'),
        ('sector_category', 'VARCHAR(20)'),
    ]:
        add_column_if_not_exists("tr_stock_pool", col_name, col_def)

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_audit_recommendations (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(50),
            name VARCHAR(255),
            sector VARCHAR(255),
            current_price DOUBLE,
            target_price DOUBLE,
            upside DOUBLE,
            opinion VARCHAR(255),
            data_date VARCHAR(50),
            created_at VARCHAR(50),
            score DOUBLE DEFAULT 0.0,
            roe DOUBLE DEFAULT 0.0,
            debt DOUBLE DEFAULT 0.0,
            reason TEXT,
            news_summary LONGTEXT,
            rec_type VARCHAR(50) DEFAULT 'momentum',
            one_liner VARCHAR(255) DEFAULT '',
            disc_json LONGTEXT
        )
    ''')
    for col_name, col_def in [
        ('rec_type', "VARCHAR(50) DEFAULT 'momentum'"),
        ('one_liner', "VARCHAR(255) DEFAULT ''"),
        ('disc_json', 'LONGTEXT'),
        ('sector', 'VARCHAR(255)'),
    ]:
        add_column_if_not_exists("tr_audit_recommendations", col_name, col_def)

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_settings (
            `key` VARCHAR(100) PRIMARY KEY,
            value VARCHAR(255)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_investment_journal (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date VARCHAR(50) UNIQUE,
            content LONGTEXT,
            created_at VARCHAR(50),
            updated_at VARCHAR(50)
        )
    ''')

    create_index_if_not_exists("idx_tr_stock_daily_history_date", "tr_stock_daily_history", "date")
    create_index_if_not_exists("idx_history_date_owner", "tr_stock_daily_history", "date, owner")
    create_index_if_not_exists("idx_journal_date", "tr_investment_journal", "date")

    # [김선화/김정음] 해외 주식 관리 테이블 (MySQL - id PK 및 broker 컬럼 지원)
    cursor.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = 'tr_overseas_stocks'
    """)
    table_exists = (cursor.fetchone()[0] > 0)

    if table_exists:
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = 'tr_overseas_stocks' AND column_name = 'id'
        """)
        has_id = (cursor.fetchone()[0] > 0)
        
        if not has_id:
            print("[DB 마이그레이션] MySQL tr_overseas_stocks 테이블을 id PK 및 broker 구조로 마이그레이션합니다.")
            cursor.execute("ALTER TABLE tr_overseas_stocks RENAME TO tr_overseas_stocks_old")
            cursor.execute('''
                CREATE TABLE tr_overseas_stocks (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    symbol VARCHAR(50),
                    name VARCHAR(255),
                    broker VARCHAR(50) DEFAULT '기본',
                    quantity DOUBLE DEFAULT 0,
                    purchase_price DOUBLE DEFAULT 0,
                    close_price DOUBLE DEFAULT 0,
                    price_change DOUBLE DEFAULT 0,
                    fluctuations_ratio DOUBLE DEFAULT 0,
                    dividend_per_share DOUBLE DEFAULT 0,
                    dividend_yield DOUBLE DEFAULT 0,
                    dividend_date VARCHAR(50),
                    ex_dividend_date VARCHAR(50),
                    exchange_rate DOUBLE DEFAULT 0,
                    currency VARCHAR(10) DEFAULT 'USD',
                    price_date VARCHAR(50),
                    updated_at VARCHAR(50),
                    notes TEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            ''')
            try:
                cursor.execute('''
                    INSERT INTO tr_overseas_stocks (symbol, name, broker, quantity, purchase_price, close_price, price_change, fluctuations_ratio, dividend_per_share, dividend_yield, dividend_date, ex_dividend_date, exchange_rate, currency, price_date, updated_at, notes)
                    SELECT symbol, name, '기본', quantity, purchase_price, close_price, price_change, fluctuations_ratio, dividend_per_share, dividend_yield, dividend_date, ex_dividend_date, exchange_rate, currency, price_date, updated_at, notes FROM tr_overseas_stocks_old
                ''')
            except Exception as e:
                print(f"[DB 마이그레이션 경고] tr_overseas_stocks 데이터 복사 중 예외: {e}")
            cursor.execute("DROP TABLE IF EXISTS tr_overseas_stocks_old")
        else:
            add_column_if_not_exists("tr_overseas_stocks", "broker", "VARCHAR(50) DEFAULT '기본'")
    else:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tr_overseas_stocks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(50),
                name VARCHAR(255),
                broker VARCHAR(50) DEFAULT '기본',
                quantity DOUBLE DEFAULT 0,
                purchase_price DOUBLE DEFAULT 0,
                close_price DOUBLE DEFAULT 0,
                price_change DOUBLE DEFAULT 0,
                fluctuations_ratio DOUBLE DEFAULT 0,
                dividend_per_share DOUBLE DEFAULT 0,
                dividend_yield DOUBLE DEFAULT 0,
                dividend_date VARCHAR(50),
                ex_dividend_date VARCHAR(50),
                exchange_rate DOUBLE DEFAULT 0,
                currency VARCHAR(10) DEFAULT 'USD',
                price_date VARCHAR(50),
                updated_at VARCHAR(50),
                notes TEXT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ''')

    # [김선화/김정음] 배당금 수령 내역 관리 테이블 (MySQL)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_dividend_history (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(50),
            name VARCHAR(255),
            broker VARCHAR(50) DEFAULT '기본',
            market_type VARCHAR(20) DEFAULT 'OVERSEAS',
            payment_date VARCHAR(50),
            amount_gross DOUBLE DEFAULT 0,
            amount_net DOUBLE DEFAULT 0,
            currency VARCHAR(10) DEFAULT 'USD',
            exchange_rate DOUBLE DEFAULT 0,
            amount_net_krw DOUBLE DEFAULT 0,
            notes TEXT,
            created_at VARCHAR(50)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    ''')
    add_column_if_not_exists("tr_dividend_history", "broker", "VARCHAR(50) DEFAULT '기본'")
    create_index_if_not_exists("idx_dividend_date", "tr_dividend_history", "payment_date")
    create_index_if_not_exists("idx_dividend_symbol", "tr_dividend_history", "symbol")

    # [김선화/김정음] 해외 주식 일별 스냅샷 히스토리 테이블 (MySQL)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_overseas_daily_history (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date VARCHAR(50),
            symbol VARCHAR(50),
            name VARCHAR(255),
            broker VARCHAR(50) DEFAULT '기본',
            quantity DOUBLE DEFAULT 0,
            purchase_price DOUBLE DEFAULT 0,
            close_price DOUBLE DEFAULT 0,
            price_change DOUBLE DEFAULT 0,
            fluctuations_ratio DOUBLE DEFAULT 0,
            exchange_rate DOUBLE DEFAULT 0,
            currency VARCHAR(10) DEFAULT 'USD',
            day_profit_usd DOUBLE DEFAULT 0,
            day_profit_krw DOUBLE DEFAULT 0,
            cumulative_profit_usd DOUBLE DEFAULT 0,
            cumulative_profit_krw DOUBLE DEFAULT 0,
            cumulative_profit_rate DOUBLE DEFAULT 0,
            eval_usd DOUBLE DEFAULT 0,
            eval_krw DOUBLE DEFAULT 0,
            cost_usd DOUBLE DEFAULT 0,
            cost_krw DOUBLE DEFAULT 0,
            recorded_at VARCHAR(50)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    ''')
    create_index_if_not_exists("idx_ov_daily_date", "tr_overseas_daily_history", "date")
    create_index_if_not_exists("idx_ov_daily_symbol_date", "tr_overseas_daily_history", "symbol, date")

    conn.commit()
    conn.close()


if __name__ == '__main__':
    init_db()
    print("Database initialization completed.")

