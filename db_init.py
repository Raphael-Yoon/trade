# -*- coding: utf-8 -*-
import os
import pymysql
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
DATABASE_URL = os.getenv('DATABASE_URL')


def init_db():
    """데이터베이스 초기화 및 테이블 생성 (MySQL)"""
    parsed = urlparse(DATABASE_URL)
    db_opts = {
        'host': parsed.hostname or '127.0.0.1',
        'port': parsed.port or 3306,
        'user': parsed.username or 'root',
        'password': parsed.password or '150606',
        'database': parsed.path.lstrip('/') if parsed.path else 'trade',
        'charset': 'utf8mb4',
    }
    conn = pymysql.connect(**db_opts)
    cursor = conn.cursor()

    # Helper function for MySQL column addition
    def add_column_if_not_exists(table, col, definition):
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_schema = DATABASE() 
              AND table_name = '{table}' 
              AND column_name = '{col}'
        """)
        if cursor.fetchone()[0] == 0:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} {definition}")

    # Helper function for MySQL index creation
    def create_index_if_not_exists(idx_name, table, col):
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.statistics 
            WHERE table_schema = DATABASE() 
              AND table_name = '{table}' 
              AND index_name = '{idx_name}'
        """)
        if cursor.fetchone()[0] == 0:
            cursor.execute(f"CREATE INDEX {idx_name} ON {table}({col})")

    # 종목 통합 테이블 (보유 종목 + 관심 종목)
    # type = 'portfolio' (보유) / 'watchlist' (관심)
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
            type VARCHAR(50) DEFAULT 'portfolio'
        )
    ''')
    for col_name, col_def in [
        ('purchase_price', 'DOUBLE DEFAULT 0'),
        ('quantity', 'INT DEFAULT 0'),
        ('stop_loss_ratio', 'DOUBLE DEFAULT 0'),
        ('is_favorite', 'INT DEFAULT 0'),
        ('peak_price', 'DOUBLE DEFAULT 0'),
        ("owner", "VARCHAR(50) DEFAULT '나'"),
        ("type", "VARCHAR(50) DEFAULT 'portfolio'"),
    ]:
        add_column_if_not_exists("tr_my_stocks", col_name, col_def)

    # 종목별 일자별 히스토리 테이블
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

    # 매도 거래 이력 테이블
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

    # 종목 마스터 테이블 (검색용)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_stocks_master (
            code VARCHAR(50) PRIMARY KEY,
            name VARCHAR(255),
            market VARCHAR(50)
        )
    ''')

    # 포트폴리오 AI 분석 캐시 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_portfolio_ai_cache (
            cache_key VARCHAR(255) PRIMARY KEY,
            ai_result LONGTEXT,
            created_at VARCHAR(50)
        )
    ''')

    # 투자 풀 테이블 (감사팀 협업)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_stock_pool (
            code VARCHAR(50) PRIMARY KEY,
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
            is_sector_leader BOOLEAN
        )
    ''')
    for col_name, col_def in [
        ('market_cap', 'DOUBLE'),
        ('is_sector_leader', 'BOOLEAN')
    ]:
        add_column_if_not_exists("tr_stock_pool", col_name, col_def)

    # 감사팀 추천 종목 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tr_audit_recommendations (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(50),
            name VARCHAR(255),
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
        ('disc_json', "LONGTEXT")
    ]:
        add_column_if_not_exists("tr_audit_recommendations", col_name, col_def)

    # 조회 성능 인덱스
    create_index_if_not_exists("idx_tr_stock_daily_history_date", "tr_stock_daily_history", "date")

    conn.commit()
    conn.close()
