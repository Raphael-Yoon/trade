# -*- coding: utf-8 -*-
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
DATABASE_URL = os.getenv('DATABASE_URL')


def init_db():
    """데이터베이스 초기화 및 테이블 생성 (PostgreSQL)"""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    # 종목 통합 테이블 (보유 종목 + 관심 종목)
    # type = 'portfolio' (보유) / 'watchlist' (관심)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS my_stocks (
            code TEXT PRIMARY KEY,
            name TEXT,
            added_at TEXT,
            purchase_price REAL DEFAULT 0,
            quantity INTEGER DEFAULT 0,
            stop_loss_ratio REAL DEFAULT 0,
            is_favorite INTEGER DEFAULT 0,
            peak_price REAL DEFAULT 0,
            owner TEXT DEFAULT '나',
            type TEXT DEFAULT 'portfolio'
        )
    ''')
    for col_name, col_def in [
        ('purchase_price', 'REAL DEFAULT 0'),
        ('quantity', 'INTEGER DEFAULT 0'),
        ('stop_loss_ratio', 'REAL DEFAULT 0'),
        ('is_favorite', 'INTEGER DEFAULT 0'),
        ('peak_price', 'REAL DEFAULT 0'),
        ("owner", "TEXT DEFAULT '나'"),
        ("type", "TEXT DEFAULT 'portfolio'"),
    ]:
        cursor.execute(f"ALTER TABLE my_stocks ADD COLUMN IF NOT EXISTS {col_name} {col_def}")

    # 종목별 일자별 히스토리 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_daily_history (
            id SERIAL PRIMARY KEY,
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
        cursor.execute(f"ALTER TABLE stock_daily_history ADD COLUMN IF NOT EXISTS {col_name} {col_def}")

    # 매도 거래 이력 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sell_history (
            id SERIAL PRIMARY KEY,
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

    # 종목 마스터 테이블 (검색용)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stocks_master (
            code TEXT PRIMARY KEY,
            name TEXT,
            market TEXT
        )
    ''')

    # 포트폴리오 AI 분석 캐시 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS portfolio_ai_cache (
            cache_key TEXT PRIMARY KEY,
            ai_result TEXT,
            created_at TEXT
        )
    ''')

    # 투자 풀 테이블 (감사팀 협업)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_pool (
            code TEXT PRIMARY KEY,
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
            updated_at TEXT
        )
    ''')

    # 감사팀 추천 종목 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS audit_recommendations (
            id SERIAL PRIMARY KEY,
            code TEXT,
            name TEXT,
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
            rec_type TEXT DEFAULT 'momentum'
        )
    ''')
    for col_name, col_def in [
        ('rec_type', "TEXT DEFAULT 'momentum'"),
        ('one_liner', "TEXT DEFAULT ''"),
    ]:
        cursor.execute(f"ALTER TABLE audit_recommendations ADD COLUMN IF NOT EXISTS {col_name} {col_def}")

    # 조회 성능 인덱스
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_daily_history_date ON stock_daily_history(date)")

    conn.commit()
    conn.close()
