# -*- coding: utf-8 -*-
import os
import sys
import io
import argparse
import json
import sqlite3
import re
from datetime import datetime
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# 프로젝트 루트 경로 추가 (drive_sync 등을 가져오기 위함)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)
sys.path.append(os.path.join(PROJECT_ROOT, 'trade'))

# 환경 변수 로드
load_dotenv(os.path.join(PROJECT_ROOT, 'trade', '.env'))
DATABASE_URL = os.getenv('DATABASE_URL')
SQLITE_PATH = os.path.join(PROJECT_ROOT, 'trade', 'trade.db')

def _new_db_conn():
    if DATABASE_URL and DATABASE_URL.startswith('mysql'):
        import pymysql
        from urllib.parse import urlparse
        parsed = urlparse(DATABASE_URL)
        return pymysql.connect(
            host=parsed.hostname or '127.0.0.1',
            port=parsed.port or 3306,
            user=parsed.username or 'root',
            password=parsed.password or '',
            database=parsed.path.lstrip('/') if parsed.path else 'trade',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
    else:
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row
        return conn

def parse_args():
    parser = argparse.ArgumentParser(description="Sector-based stock pool generator")
    parser.add_argument('--source_file', required=True, help="Original spreadsheet file name")
    parser.add_argument('--id', required=False, help="Google Drive Spreadsheet ID")
    parser.add_argument('--file', required=False, help="Local file path")
    return parser.parse_args()

def main():
    args = parse_args()
    print(f"[*] Starting Pool Generation for source_file: {args.source_file}")
    
    df = None
    
    # 1. 데이터 로드 (드라이브 우선, 없으면 로컬 파일)
    if args.id:
        print(f"[*] Downloading from Google Drive ID: {args.id}...")
        try:
            from drive_sync import download_from_drive
            content = download_from_drive(args.id)
            if content:
                df = pd.read_excel(io.BytesIO(content))
                print("[+] Successfully loaded from Drive.")
        except Exception as e:
            print(f"[!] Drive download failed: {e}")
            
    if df is None and args.file:
        print(f"[*] Loading from local file: {args.file}...")
        if os.path.exists(args.file):
            df = pd.read_excel(args.file)
            print("[+] Successfully loaded from local file.")
        else:
            print(f"[!] Local file not found: {args.file}")
            
    if df is None:
        print("[ERROR] Failed to load any data.")
        sys.exit(1)
        
    # 필수 컬럼 체크
    required_cols = ['종목코드', '종목명', '업종']
    for col in required_cols:
        if col not in df.columns:
            print(f"[ERROR] Missing required column: {col}")
            sys.exit(1)

    # 1-1. 회계감사의견 필터링 — '적정의견'이 아닌 종목(의견거절, 한정의견 등)은 Pool 구성 자체에서 제외
    # 감사의견 데이터가 없는 종목(N/A)은 "부적정으로 확인된" 것이 아니라 데이터 결측이므로 제외하지 않는다.
    if '회계감사의견' in df.columns:
        before_count = len(df)
        # 문자열 공백 제거 안전망 추가
        df['회계감사의견'] = df['회계감사의견'].apply(lambda x: str(x).strip() if pd.notna(x) else x)
        disqualified = df[~df['회계감사의견'].isin(['적정의견', 'N/A']) & df['회계감사의견'].notna()]
        if not disqualified.empty:
            print(f"[*] 감사의견 부적정으로 제외된 종목: {len(disqualified)}건 "
                  f"({disqualified['회계감사의견'].value_counts().to_dict()})")
        df = df[df['회계감사의견'].isin(['적정의견', 'N/A']) | df['회계감사의견'].isna()]
        print(f"[*] 감사의견 필터링: {before_count}건 -> {len(df)}건")
    else:
        print("[!] 경고: '회계감사의견' 컬럼이 없어 감사의견 필터링을 건너뜁니다.")

    # 2. 데이터 전처리 및 수치 변환
    numeric_cols = ['PBR', 'PER', 'ROE', '시가총액', '매출액', '영업이익', '당기순이익', 
                    '부채비율', '매출액증가율(%)', '영업이익증가율(%)', '순이익증가율(%)', '영업이익률', '순이익률']
    for col in numeric_cols:
        if col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.replace(',', '').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            # 없는 컬럼은 NaN으로 임시 생성
            df[col] = np.nan

    # 3. 스코어 계산 — 섹터 내 백분위 순위 기반 복합 점수
    # 단일 지표(ROE-PBR 비율 또는 영업이익 절대 규모)만 쓰면 한쪽으로 쏠린다:
    #   - 비율(ROE-PBR)만 쓰면 삼성전자·SK하이닉스처럼 밸류에이션이 높게 형성된
    #     압도적 대형주가 오히려 제외됨.
    #   - 절대 규모(영업이익)만 쓰면 사실상 몸집 순위가 되어 밸류에이션/성장성/건전성
    #     데이터가 무의미해짐.
    #   - 5개 지표를 동일 가중치로 평균하면, 밸류에이션 한 항목만 나빠도 압도적
    #     대형주가 중위권으로 밀려남 (실측 결과 삼성전자 5위, 한화오션 10위 밖 제외).
    # 따라서 규모(영업이익)에 절반 가중치를 두어 섹터 대표주가 항상 상위권을
    # 유지하게 하고, 나머지 절반을 수익성·밸류에이션·성장성·재무건전성으로
    # 세분화해 동일 규모대 종목 간 우열을 가린다.
    df['영업이익_fill'] = df['영업이익'].fillna(0.0)
    df['ROE_fill'] = df['ROE'].fillna(0.0)
    # 업종(섹터)별 PBR 및 부채비율 중위값으로 채우고, 업종 전체가 결측일 때만 시장 전체 중위값 적용
    df['PBR_fill'] = df.groupby('업종')['PBR'].transform(lambda x: x.fillna(x.median())).fillna(df['PBR'].median())
    df['영업이익증가율_fill'] = df['영업이익증가율(%)'].fillna(0.0)
    df['부채비율_fill'] = df.groupby('업종')['부채비율'].transform(lambda x: x.fillna(x.median())).fillna(df['부채비율'].median())

    grp = df.groupby('업종')
    scale_pct = grp['영업이익_fill'].rank(pct=True)                # 규모: 클수록 좋음
    roe_pct = grp['ROE_fill'].rank(pct=True)                       # 수익성: 클수록 좋음
    valuation_pct = 1 - grp['PBR_fill'].rank(pct=True)             # 밸류에이션: PBR 낮을수록 좋음
    growth_pct = grp['영업이익증가율_fill'].rank(pct=True)          # 성장성: 클수록 좋음
    health_pct = 1 - grp['부채비율_fill'].rank(pct=True)            # 건전성: 부채비율 낮을수록 좋음

    W_SCALE, W_ROE, W_VALUATION, W_GROWTH, W_HEALTH = 0.5, 0.15, 0.15, 0.1, 0.1
    df['pool_score'] = (
        scale_pct * W_SCALE + roe_pct * W_ROE + valuation_pct * W_VALUATION
        + growth_pct * W_GROWTH + health_pct * W_HEALTH
    ) * 100

    # 4. 주요 섹터 식별 (규모 상위 4개 + 성장 상위 4개 = 8개)
    # 영업이익 합계 단독 기준으로만 뽑으면 "이미 큰 섹터"만 계속 뽑히고, 화장품·백화점처럼
    # 아직 규모는 작지만 빠르게 크는 섹터는 영영 후보에 못 낀다. 그렇다고 성장률만 보면
    # 종목 1~2개짜리 미니 섹터가 기저효과로 왜곡되어 뽑힌다(실측 확인됨).
    # → 종목수 10개 이상인 섹터만 후보로 삼고, 그중 (a) 영업이익 합계 상위 4개(규모 대표)
    #   + (b) 영업이익증가율·매출증가율 중앙값 평균 상위 4개(성장 대표)를 합쳐 8개를 구성한다.
    #   중앙값을 쓰는 이유: 평균은 소수 종목의 급등(기저효과)에 취약함이 실측으로 확인됨.
    MIN_SECTOR_STOCKS = 10
    sector_stats = df.groupby('업종').agg(
        종목수=('영업이익_fill', 'count'),
        영업이익합계=('영업이익_fill', 'sum'),
        영업이익증가율_중앙값=('영업이익증가율(%)', 'median'),
        매출증가율_중앙값=('매출액증가율(%)', 'median'),
    ).reset_index()
    sector_stats['성장복합'] = (
        sector_stats['영업이익증가율_중앙값'].fillna(0.0) + sector_stats['매출증가율_중앙값'].fillna(0.0)
    ) / 2

    eligible = sector_stats[sector_stats['종목수'] >= MIN_SECTOR_STOCKS]

    top4_scale = eligible.sort_values(by='영업이익합계', ascending=False).head(4)['업종'].tolist()
    remaining = eligible[~eligible['업종'].isin(top4_scale)]
    top4_growth = remaining.sort_values(by='성장복합', ascending=False).head(4)['업종'].tolist()

    top_sectors = top4_scale + top4_growth
    print(f"[*] Top 4 Sectors by Operating Income: {top4_scale}")
    print(f"[*] Top 4 Sectors by Growth (OpIncome+Revenue median): {top4_growth}")

    # 5. 종목 선발 (섹터별 쿼터제)
    # sector_category: 화면 섹터탭에서 "규모 대표/성장 대표/기타"를 구분해 표시하기 위한 태그
    selected_dfs = []

    # 규모 대표 섹터 선발 (섹터당 복합 스코어 상위 10개)
    for sector in top4_scale:
        sector_df = df[df['업종'] == sector]
        leaders = sector_df.sort_values(by='pool_score', ascending=False).head(10).copy()
        leaders['is_sector_leader'] = 1
        leaders['sector_category'] = 'scale'
        selected_dfs.append(leaders)

    # 성장 대표 섹터 선발 (섹터당 복합 스코어 상위 10개)
    for sector in top4_growth:
        sector_df = df[df['업종'] == sector]
        leaders = sector_df.sort_values(by='pool_score', ascending=False).head(10).copy()
        leaders['is_sector_leader'] = 1
        leaders['sector_category'] = 'growth'
        selected_dfs.append(leaders)

    # 기타 섹터 선발 (주요 8대 섹터를 제외한 모든 섹터 통합, 복합 스코어 상위 20개)
    other_df = df[~df['업종'].isin(top_sectors)]
    others = other_df.sort_values(by='pool_score', ascending=False).head(20).copy()
    if not others.empty:
        others['is_sector_leader'] = 0
        others['sector_category'] = 'other'
        selected_dfs.append(others)

    if not selected_dfs:
        print("[ERROR] No stocks selected.")
        sys.exit(1)

    final_pool = pd.concat(selected_dfs)
    print(f"[+] Total selected stocks for pool: {len(final_pool)}")
    
    # 6. DB 적재
    conn = _new_db_conn()
    cursor = conn.cursor()
    
    # 기존 데이터 삭제 (source_file 기준)
    delete_sql = "DELETE FROM tr_stock_pool WHERE source_file = ?"
    if DATABASE_URL and DATABASE_URL.startswith('mysql'):
        cursor.execute(delete_sql, (args.source_file,))
    else:
        cursor.execute(delete_sql, (args.source_file,))
        
    insert_sql = """
        INSERT INTO tr_stock_pool (
            code, name, sector, roe, pbr, per, debt_ratio, operating_margin,
            target_price, pool_score, data_date, updated_at, market_cap,
            is_sector_leader, source_file, sector_category
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    # 데이터 날짜 파싱 (파일명 내 YYYYMMDD 날짜가 있으면 파싱, 없으면 오늘 날짜)
    data_date = datetime.now().strftime('%Y-%m-%d')
    date_match = re.search(r'\d{8}', args.source_file)
    if date_match:
        try:
            raw_date = date_match.group(0)
            data_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
        except Exception:
            pass
            
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 튜플 구성 및 적재
    insert_data = []
    for _, row in final_pool.iterrows():
        # 종목코드 포맷팅 (6자리 보장)
        code_str = str(row['종목코드']).strip()
        if len(code_str) < 6:
            code_str = code_str.zfill(6)
            
        insert_data.append((
            code_str,
            str(row['종목명']),
            str(row['업종']),
            float(row['ROE']) if not pd.isna(row['ROE']) else 0.0,
            float(row['PBR']) if not pd.isna(row['PBR']) else 0.0,
            float(row['PER']) if not pd.isna(row['PER']) else 0.0,
            float(row['부채비율']) if not pd.isna(row['부채비율']) else 0.0,
            float(row['영업이익률']) if not pd.isna(row['영업이익률']) else 0.0,
            float(row['목표주가']) if not pd.isna(row['목표주가']) else 0.0,
            float(row['pool_score']),
            data_date,
            now_str,
            float(row['시가총액']) if not pd.isna(row['시가총액']) else 0.0,
            int(row['is_sector_leader']),
            args.source_file,
            str(row['sector_category'])
        ))
        
    try:
        if DATABASE_URL and DATABASE_URL.startswith('mysql'):
            cursor.executemany(insert_sql.replace('?', '%s'), insert_data)
        else:
            cursor.executemany(insert_sql, insert_data)
        conn.commit()
        print(f"[+] Successfully saved {len(insert_data)} records to database.")
    except Exception as e:
        print(f"[ERROR] Database insert failed: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == '__main__':
    main()
