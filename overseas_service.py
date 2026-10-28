"""
조나단 커피하우스 — 해외 주식 및 배당금 관리 서비스 모듈
담당: 개발2팀 김정음(백엔드), 김도희(퀀트), 권보성(QA)
"""

import time
import datetime
import threading
import logging
import requests

logger = logging.getLogger("overseas_service")

# 기본 Fallback 환율 (네트워크 장애 대비)
FALLBACK_USD_KRW = 1350.0

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def fetch_usd_krw_rate() -> float:
    """네이버 금융 모바일 API를 통한 원/달러(USD/KRW) 실시간 환율 조회"""
    try:
        url = "https://m.stock.naver.com/front-api/marketIndex/productDetail?category=exchange&reutersCode=FX_USDKRW"
        resp = requests.get(url, headers=HEADERS, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            close_price_str = data.get("result", {}).get("closePrice", "")
            if close_price_str:
                rate = float(str(close_price_str).replace(",", ""))
                return rate
    except Exception as e:
        logger.warning(f"[overseas_service] 환율 조회 실패 ({e}), Fallback 사용: {FALLBACK_USD_KRW}")
    return FALLBACK_USD_KRW


def search_overseas_stock(query: str):
    """네이버 증권 자동완성 API를 통한 해외 주식 종목/티커 검색"""
    if not query or len(query.strip()) < 1:
        return []

    try:
        url = f"https://ac.stock.naver.com/ac?q={requests.utils.quote(query.strip())}&target=stock"
        resp = requests.get(url, headers=HEADERS, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("items", [])
            results = []
            for item in items:
                # 해외 주식 필터링 (reutersCode가 있고 nationCode가 있거나 국내 6자리가 아닌 경우)
                reuters_code = item.get("reutersCode") or ""
                nation_name = item.get("nationName") or ""
                type_name = item.get("typeName") or ""
                
                # 국내주식이 아닌 해외주식 우선 혹은 포함
                if reuters_code and (nation_name != "대한민국" or "." in reuters_code):
                    results.append({
                        "symbol": reuters_code,
                        "code": item.get("code", ""),
                        "name": item.get("name", ""),
                        "market": type_name,
                        "nation": nation_name
                    })
            return results
    except Exception as e:
        logger.error(f"[overseas_service] 해외주식 검색 실패: {e}")
    return []


def _safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        if isinstance(val, (int, float)):
            return float(val)
        cleaned = str(val).replace(",", "").replace("%", "").replace("배", "").strip()
        if not cleaned or cleaned.upper() == "N/A":
            return default
        return float(cleaned)
    except Exception:
        return default


def fetch_overseas_stock_detail(symbol: str):
    """
    네이버 증권 해외주식 상세 API를 통한 시세 및 배당 정보 수집
    symbol 예시: 'AAPL.O', 'TSLA.O', 'NVDA.O' (혹은 AAPL 입력 시 .O 붙여서 시도)
    """
    symbol = symbol.strip().upper()
    target_symbols = [symbol]
    if "." not in symbol:
        # 미국 주식 대표 거래소 접미사 자동 시도
        target_symbols = [f"{symbol}.O", f"{symbol}.K", f"{symbol}.A", symbol]

    for sym in target_symbols:
        try:
            url = f"https://api.stock.naver.com/stock/{sym}/basic"
            resp = requests.get(url, headers=HEADERS, timeout=6)
            if resp.status_code == 200:
                data = resp.json()
                if not data or not data.get("stockName"):
                    continue

                stock_name = data.get("stockName", "")
                stock_name_eng = data.get("stockNameEng", "")
                reuters_code = data.get("reutersCode", sym)
                close_price = _safe_float(data.get("closePrice"))
                price_change = _safe_float(data.get("compareToPreviousClosePrice"))
                fluctuations_ratio = _safe_float(data.get("fluctuationsRatio"))
                local_traded_at = data.get("localTradedAt", "")

                # 배당 정보 추출 (stockItemTotalInfos)
                dividend_per_share = 0.0
                dividend_yield = 0.0
                dividend_date = ""
                ex_dividend_date = ""

                total_infos = data.get("stockItemTotalInfos") or []
                for item in total_infos:
                    code = item.get("code")
                    val = item.get("value")
                    if code == "dividend":
                        dividend_per_share = _safe_float(val)
                    elif code == "dividendYieldRatio":
                        dividend_yield = _safe_float(val)
                    elif code == "dividendAt":
                        dividend_date = str(val or "").strip()
                    elif code == "exDividendAt":
                        ex_dividend_date = str(val or "").strip()

                return {
                    "symbol": reuters_code,
                    "name": stock_name,
                    "name_eng": stock_name_eng,
                    "close_price": close_price,
                    "price_change": price_change,
                    "fluctuations_ratio": fluctuations_ratio,
                    "dividend_per_share": dividend_per_share,
                    "dividend_yield": dividend_yield,
                    "dividend_date": dividend_date,
                    "ex_dividend_date": ex_dividend_date,
                    "price_date": local_traded_at,
                    "currency": "USD"
                }
        except Exception as e:
            logger.warning(f"[overseas_service] {sym} 조회 시도 실패: {e}")
            continue

    return None


def update_all_overseas_stocks(db_conn):
    """등록된 모든 해외 주식의 종가, 배당정보, 환율을 최신화"""
    cursor = db_conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM tr_overseas_stocks WHERE symbol IS NOT NULL AND symbol != ''")
    rows = cursor.fetchall()
    if not rows:
        return {"updated": 0, "total": 0, "exchange_rate": fetch_usd_krw_rate()}

    rate = fetch_usd_krw_rate()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updated_count = 0
    errors = []

    for row in rows:
        sym = row.get("symbol") if isinstance(row, dict) else row[0]
        try:
            detail = fetch_overseas_stock_detail(sym)
            if detail:
                cursor.execute("""
                    UPDATE tr_overseas_stocks
                    SET name = ?,
                        close_price = ?,
                        price_change = ?,
                        fluctuations_ratio = ?,
                        dividend_per_share = ?,
                        dividend_yield = ?,
                        dividend_date = ?,
                        ex_dividend_date = ?,
                        exchange_rate = ?,
                        price_date = ?,
                        updated_at = ?
                    WHERE symbol = ?
                """, (
                    detail["name"],
                    detail["close_price"],
                    detail["price_change"],
                    detail["fluctuations_ratio"],
                    detail["dividend_per_share"],
                    detail["dividend_yield"],
                    detail["dividend_date"],
                    detail["ex_dividend_date"],
                    rate,
                    detail["price_date"],
                    now_str,
                    sym
                ))
                updated_count += 1
            else:
                errors.append(sym)
            time.sleep(0.3)  # API 예의 호출
        except Exception as e:
            logger.error(f"[overseas_service] {sym} 업데이트 실패: {e}")
            errors.append(sym)

    db_conn.commit()
    logger.info(f"[overseas_service] 해외주식 업데이트 완료: {updated_count}/{len(rows)} (환율: {rate})")

    # 일별 평가 스냅샷 자동 기록 (히스토리 축적)
    snapshot_count = save_overseas_daily_snapshot(db_conn)

    return {
        "updated": updated_count,
        "total": len(rows),
        "exchange_rate": rate,
        "errors": errors,
        "updated_at": now_str,
        "snapshot_count": snapshot_count
    }


def save_overseas_daily_snapshot(db_conn, target_date=None):
    """보유 해외주식의 일별 평가 스냅샷을 tr_overseas_daily_history에 저장 (국내주식 daily history와 동급)"""
    try:
        cursor = db_conn.cursor()
        today = target_date or datetime.datetime.now().strftime("%Y-%m-%d")
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute("""
            SELECT symbol, name, broker, quantity, purchase_price, close_price,
                   price_change, fluctuations_ratio, exchange_rate, currency
            FROM tr_overseas_stocks
        """)
        rows = cursor.fetchall()
        if not rows:
            return 0

        # 당일 기존 스냅샷 삭제 후 재적재 (idempotent)
        cursor.execute("DELETE FROM tr_overseas_daily_history WHERE date = ?", (today,))

        inserted = 0
        for r in rows:
            symbol = r.get("symbol") if isinstance(r, dict) else r[0]
            name = r.get("name") if isinstance(r, dict) else r[1]
            broker = (r.get("broker") if isinstance(r, dict) else r[2]) or '기본'
            qty = float((r.get("quantity") if isinstance(r, dict) else r[3]) or 0.0)
            purchase_price = float((r.get("purchase_price") if isinstance(r, dict) else r[4]) or 0.0)
            close_price = float((r.get("close_price") if isinstance(r, dict) else r[5]) or 0.0)
            price_change = float((r.get("price_change") if isinstance(r, dict) else r[6]) or 0.0)
            fluct_ratio = float((r.get("fluctuations_ratio") if isinstance(r, dict) else r[7]) or 0.0)
            rate = float((r.get("exchange_rate") if isinstance(r, dict) else r[8]) or 1350.0)
            currency = (r.get("currency") if isinstance(r, dict) else r[9]) or 'USD'

            eval_usd = round(close_price * qty, 2)
            eval_krw = round(eval_usd * rate)
            cost_usd = round(purchase_price * qty, 2)
            cost_krw = round(cost_usd * rate)
            day_profit_usd = round(price_change * qty, 2)
            day_profit_krw = round(day_profit_usd * rate)
            cum_profit_usd = round(eval_usd - cost_usd, 2)
            cum_profit_krw = round(eval_krw - cost_krw)
            cum_profit_rate = round((cum_profit_usd / cost_usd * 100), 2) if cost_usd > 0 else 0.0

            cursor.execute("""
                INSERT INTO tr_overseas_daily_history (
                    date, symbol, name, broker, quantity, purchase_price, close_price,
                    price_change, fluctuations_ratio, exchange_rate, currency,
                    day_profit_usd, day_profit_krw, cumulative_profit_usd, cumulative_profit_krw,
                    cumulative_profit_rate, eval_usd, eval_krw, cost_usd, cost_krw, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                today, symbol, name, broker, qty, purchase_price, close_price,
                price_change, fluct_ratio, rate, currency,
                day_profit_usd, day_profit_krw, cum_profit_usd, cum_profit_krw,
                cum_profit_rate, eval_usd, eval_krw, cost_usd, cost_krw, now_str
            ))
            inserted += 1

        db_conn.commit()
        logger.info(f"[overseas_service] 해외주식 일별 스냅샷 저장 완료: {today} ({inserted}건)")
        return inserted
    except Exception as e:
        logger.error(f"[overseas_service] 해외주식 일별 스냅샷 저장 실패: {e}")
        return 0


def start_overseas_daily_scheduler(get_db_conn_fn):
    """
    매일 오전 08:30 (KST, 미국 정규장 마감 후)에 1회 종가 및 배당 정보를 자동 갱신하는 백그라운드 데몬 스레드
    """
    def _worker():
        logger.info("[overseas_service] 해외주식 일일 오전 자동 갱신 스케줄러 시작됨 (매일 08:30 KST 실행 대기)")
        last_run_date = None
        while True:
            try:
                now = datetime.datetime.now()
                today_str = now.strftime("%Y-%m-%d")
                # 오전 08:30 ~ 09:30 사이에 오늘 아직 실행 안 했으면 1회 실행
                if now.hour == 8 and now.minute >= 30 and last_run_date != today_str:
                    logger.info(f"[overseas_service] 정기 일일 오전 업데이트 트리거: {now}")
                    conn = None
                    try:
                        conn = get_db_conn_fn()
                        update_all_overseas_stocks(conn)
                        last_run_date = today_str
                    finally:
                        if conn:
                            conn.close()
                time.sleep(60)  # 1분 단위 체크
            except Exception as e:
                logger.error(f"[overseas_service] 스케줄러 루프 오류: {e}")
                time.sleep(60)

    t = threading.Thread(target=_worker, daemon=True, name="OverseasStockScheduler")
    t.start()
    return t
