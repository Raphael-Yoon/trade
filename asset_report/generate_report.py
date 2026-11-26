# -*- coding: utf-8 -*-
"""
조나단 커피하우스 — 독립형 자산 및 배당금 현황 보고서
위치: trade/asset_report/generate_report.py
담당: 개발2팀 전원 (PM 김선화)
목적: 실시간 주가/환율 기반 자산 현황과 이번 달 배당금 수령 현황을 군더더기 없이 정확히 제시
"""

import os
import sys
import json
import argparse
from datetime import datetime
from urllib.parse import urlparse
import requests

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
TRADE_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(TRADE_DIR)

import overseas_service

OUTPUT_DIR = os.path.join(CURRENT_DIR, "outputs")
EXPENSES_PATH = os.path.join(CURRENT_DIR, "expenses.json")
NOTES_PATH = os.path.join(CURRENT_DIR, "notes.json")

EXPENSE_BROKER_ORDER = [
    ("KB증권", "KB증권"),
    ("한국투자", "한국투자증권"),
    ("미래에셋", "미래에셋증권"),
]


def _load_json_dict(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
    return {}


def load_expenses():
    return _load_json_dict(EXPENSES_PATH)


def load_notes(target_month):
    notes_all = _load_json_dict(NOTES_PATH)
    entry = notes_all.get(target_month)
    return entry if isinstance(entry, dict) else None

def get_db_connection():
    env_path = os.path.join(TRADE_DIR, ".env")
    db_url = None
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip().startswith("DATABASE_URL="):
                    db_url = line.strip().split("=", 1)[1].strip()
                    break

    if db_url and db_url.startswith("mysql"):
        try:
            import pymysql
            parsed = urlparse(db_url)
            conn = pymysql.connect(
                host=parsed.hostname or "127.0.0.1",
                port=parsed.port or 3306,
                user=parsed.username or "root",
                password=parsed.password or "",
                database=parsed.path.lstrip("/"),
                cursorclass=pymysql.cursors.DictCursor
            )
            return conn, "mysql"
        except Exception:
            pass

    import sqlite3
    sqlite_path = os.path.join(TRADE_DIR, "trade.db")
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    return conn, "sqlite"

def fetch_live_domestic(code):
    url = f"https://m.stock.naver.com/api/stock/{code}/basic"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            data = r.json()
            close_price = float(str(data.get('closePrice', '0')).replace(',', ''))
            change_price = float(str(data.get('compareToPreviousClosePrice', '0')).replace(',', ''))
            change_ratio = float(str(data.get('fluctuationsRatio', '0')).replace(',', ''))
            return close_price, change_price, change_ratio
    except Exception:
        pass
    return None, 0, 0

def run_monthly_report(target_month=None):
    now = datetime.now()
    if not target_month:
        target_month = now.strftime("%Y-%m")
    
    year_str, month_str = target_month.split("-")[:2]
    edition_title = f"{year_str}년 {int(month_str)}월"

    conn, db_type = get_db_connection()
    cursor = conn.cursor()

    # 1. 국내 주식 ('나' 보유분)
    cursor.execute("""
        SELECT code, name, quantity, purchase_price 
        FROM tr_my_stocks 
        WHERE owner='나' AND type='portfolio' AND quantity > 0
    """)
    domestic_rows = cursor.fetchall()

    domestic_stocks = []
    tot_dom_cost = 0
    tot_dom_eval = 0

    for r in domestic_rows:
        code = r['code']
        name = r['name']
        qty = r['quantity']
        cost_unit = r['purchase_price']
        
        live_price, chg_price, chg_ratio = fetch_live_domestic(code)
        if not live_price:
            live_price = cost_unit

        eval_val = live_price * qty
        cost_val = cost_unit * qty
        profit = eval_val - cost_val
        profit_rate = (profit / cost_val * 100) if cost_val > 0 else 0

        tot_dom_cost += cost_val
        tot_dom_eval += eval_val

        domestic_stocks.append({
            'code': code,
            'name': name,
            'quantity': qty,
            'purchase_price': cost_unit,
            'current_price': live_price,
            'eval_value': eval_val,
            'cost_value': cost_val,
            'profit': profit,
            'profit_rate': profit_rate,
            'change_ratio': chg_ratio
        })

    # 2. 해외 주식 (AGNC)
    cursor.execute("""
        SELECT id, symbol, name, broker, quantity, purchase_price, close_price, dividend_per_share, exchange_rate, ex_dividend_date, dividend_date 
        FROM tr_overseas_stocks
    """)
    overseas_rows = cursor.fetchall()
    conn.close()

    fx_rate = overseas_service.fetch_usd_krw_rate() or 1335.3
    agnc_detail = overseas_service.fetch_overseas_stock_detail('AGNC.O') or {}
    agnc_live_price = agnc_detail.get('close_price') or 8.95
    ex_div_date = agnc_detail.get('ex_dividend_date') or f"{year_str}.{month_str}.30."
    pay_date = agnc_detail.get('dividend_date') or f"{year_str}.{int(month_str)+1:02d}.10."

    overseas_stocks = []
    tot_ovs_qty = 0
    tot_ovs_cost_usd = 0
    tot_ovs_eval_usd = 0

    monthly_dps = 0.12 # 주당 월 배당금
    tax_rate = 0.15    # 미국 현지 원천징수세 15%

    for r in overseas_rows:
        qty = float(r['quantity'])
        cost_unit = float(r['purchase_price'])
        cost_usd = qty * cost_unit
        eval_usd = qty * agnc_live_price
        
        gross_div_usd = qty * monthly_dps
        net_div_usd = gross_div_usd * (1 - tax_rate)
        net_div_krw = net_div_usd * fx_rate

        tot_ovs_qty += qty
        tot_ovs_cost_usd += cost_usd
        tot_ovs_eval_usd += eval_usd

        overseas_stocks.append({
            'broker': r['broker'],
            'name': r['name'],
            'symbol': r['symbol'],
            'quantity': qty,
            'purchase_price': cost_unit,
            'current_price': agnc_live_price,
            'cost_usd': cost_usd,
            'eval_usd': eval_usd,
            'eval_krw': eval_usd * fx_rate,
            'profit_usd': eval_usd - cost_usd,
            'profit_rate': ((eval_usd - cost_usd) / cost_usd * 100) if cost_usd > 0 else 0,
            'net_div_usd': net_div_usd,
            'net_div_krw': net_div_krw
        })

    tot_ovs_eval_krw = tot_ovs_eval_usd * fx_rate
    tot_ovs_cost_krw = tot_ovs_cost_usd * fx_rate
    tot_ovs_profit_krw = tot_ovs_eval_krw - tot_ovs_cost_krw
    tot_ovs_profit_rate = (tot_ovs_profit_krw / tot_ovs_cost_krw * 100) if tot_ovs_cost_krw > 0 else 0

    # 통합 자산 지표
    grand_total_eval_krw = tot_dom_eval + tot_ovs_eval_krw
    grand_total_cost_krw = tot_dom_cost + tot_ovs_cost_krw
    grand_total_profit_krw = grand_total_eval_krw - grand_total_cost_krw
    grand_total_profit_rate = (grand_total_profit_krw / grand_total_cost_krw * 100) if grand_total_cost_krw > 0 else 0
    dom_ratio = (tot_dom_eval / grand_total_eval_krw * 100) if grand_total_eval_krw > 0 else 0
    ovs_ratio = (tot_ovs_eval_krw / grand_total_eval_krw * 100) if grand_total_eval_krw > 0 else 0

    # 이번 달 배당금 지표
    gross_div_usd = tot_ovs_qty * monthly_dps
    gross_div_krw = gross_div_usd * fx_rate
    net_div_usd = gross_div_usd * (1 - tax_rate)
    net_div_krw = net_div_usd * fx_rate

    avg_ovs_cost_unit = (tot_ovs_cost_usd / tot_ovs_qty) if tot_ovs_qty > 0 else 0
    cur_yield = (1.44 / agnc_live_price) * 100 if agnc_live_price > 0 else 0

    # 지출 현황 (Slide 3: 원화 전용, KB증권 -> 한국투자증권 -> 미래에셋증권 순)
    expenses_data = load_expenses()
    expense_columns = []
    for db_broker, display_name in EXPENSE_BROKER_ORDER:
        div_krw = round(sum(
            s['net_div_krw'] for s in overseas_stocks if s['broker'] == db_broker
        ))
        raw_entry = expenses_data.get(display_name, [])
        if isinstance(raw_entry, dict):
            purpose = str(raw_entry.get('purpose', '') or '').strip()
            target = str(raw_entry.get('target', '') or '').strip()
            track_amounts = bool(raw_entry.get('track_amounts', True))
            note = str(raw_entry.get('note', '') or '').strip()
            total_amount_raw = raw_entry.get('total_amount', None)
            raw_items = raw_entry.get('items', [])
        elif isinstance(raw_entry, list):
            purpose = ""
            target = ""
            track_amounts = True
            note = ""
            total_amount_raw = None
            raw_items = raw_entry
        else:
            purpose = ""
            target = ""
            track_amounts = True
            note = ""
            total_amount_raw = None
            raw_items = []

        items = []
        for item in raw_items:
            if isinstance(item, dict):
                items.append({
                    'name': str(item.get('name', '')),
                    'amount': round(float(item.get('amount', 0) or 0)),
                    'date': str(item.get('date', '') or '').strip(),
                })
        itemized_sum = sum(it['amount'] for it in items)
        has_total_override = total_amount_raw is not None
        spent_krw = round(float(total_amount_raw)) if has_total_override else itemized_sum
        other_krw = spent_krw - itemized_sum
        remain_krw = div_krw - spent_krw
        expense_columns.append({
            'db_broker': db_broker,
            'display_name': display_name,
            'purpose': purpose,
            'target': target,
            'track_amounts': track_amounts,
            'note': note,
            'div_krw': div_krw,
            'items': items,
            'has_total_override': has_total_override,
            'other_krw': other_krw,
            'spent_krw': spent_krw,
            'remain_krw': remain_krw,
        })

    tot_exp_div_krw = sum(c['div_krw'] for c in expense_columns)
    tot_exp_spent_krw = sum(c['spent_krw'] for c in expense_columns)
    tot_exp_remain_krw = tot_exp_div_krw - tot_exp_spent_krw
    tot_exp_remain_color = "var(--cyan)" if tot_exp_remain_krw >= 0 else "var(--rose)"

    def _purpose_badge(purpose_text):
        if not purpose_text:
            return ""
        return (
            f'<span style="font-size: 13px; font-weight: 700; color: var(--emerald); '
            f'background: rgba(16, 185, 129, 0.14); border: 1px solid rgba(16, 185, 129, 0.35); '
            f'padding: 2px 9px; border-radius: 999px; white-space: nowrap;">{purpose_text}</span>'
        )

    expense_meta_by_broker = {c['db_broker']: c for c in expense_columns}

    expense_cols_html = ""
    for col in expense_columns:
        purpose_badge_html = _purpose_badge(col['purpose'])
        target_badge_html = (
            f'<div style="font-size: 14px; color: var(--cyan); margin-top: 2px;">→ {col["target"]}</div>'
            if col['target'] else ""
        )
        if not col['track_amounts']:
            note_text = col['note'] or "별도 사용액 미기록"
            body_html = f"""
          <div style="flex: 1; display: flex; flex-direction: column; justify-content: center; align-items: center; padding: 24px 18px; text-align: center; gap: 12px;">
            <div style="font-size: 14px; font-weight: 600; color: var(--text-muted); letter-spacing: 0.04em;">지출 매핑</div>
            <div style="font-size: 24px; font-weight: 800; color: #fff; background: rgba(56, 189, 248, 0.1); border: 1px solid rgba(56, 189, 248, 0.35); border-radius: 10px; padding: 14px 20px; width: 100%;">
              {col['target'] or '매핑 미지정'}
            </div>
            <div style="font-size: 14px; color: var(--text-dim);">{note_text}</div>
          </div>"""
            footer_html = f"""
          <div style="border-top: 1px solid var(--border-card); padding: 14px 20px; background: rgba(255, 255, 255, 0.03); display: flex; flex-direction: column; gap: 8px; font-size: 15px;">
            <div style="display: flex; justify-content: space-between; color: var(--text-muted);">
              <span>사용액 기록</span>
              <span style="font-weight: 600; color: var(--text-dim);">별도 미기록</span>
            </div>
            <div style="display: flex; justify-content: space-between; align-items: center;">
              <span style="color: var(--text-muted);">배정 배당금</span>
              <span style="font-size: 19px; font-weight: 800; color: var(--emerald);">{col['div_krw']:,}원</span>
            </div>
          </div>"""
        else:
            if col['items'] or col['other_krw'] != 0:
                item_rows_html = "".join(
                    f"""<tr>
                  <td style="color: var(--text-muted); font-size: 13.5px; white-space: nowrap; padding: 9px 14px;">{it['date'] or '-'}</td>
                  <td style="padding: 9px 14px;">{it['name']}</td>
                  <td style="text-align: right; font-weight: 600; padding: 9px 14px;">{it['amount']:,}원</td>
                </tr>"""
                    for it in col['items']
                )
                if col['other_krw'] != 0:
                    item_rows_html += f"""<tr>
                  <td style="color: var(--text-dim); font-size: 13.5px; padding: 9px 14px;">-</td>
                  <td style="color: var(--text-muted); padding: 9px 14px;">기타 사용액 (주요 내역 외)</td>
                  <td style="text-align: right; font-weight: 600; color: var(--text-muted); padding: 9px 14px;">{col['other_krw']:,}원</td>
                </tr>"""
            else:
                item_rows_html = """<tr>
              <td colspan="3" style="text-align: center; padding: 28px 12px; color: var(--text-dim);">등록된 지출 내역 없음</td>
            </tr>"""
            col_remain_color = "var(--cyan)" if col['remain_krw'] >= 0 else "var(--rose)"
            spent_label = "지출 총액" if col['has_total_override'] else "지출 합계"
            body_html = f"""
          <div style="flex: 1; overflow-y: auto;">
            <table>
              <thead>
                <tr>
                  <th style="width: 72px; padding: 10px 14px;">일자</th>
                  <th style="padding: 10px 14px;">지출 항목</th>
                  <th style="text-align: right; padding: 10px 14px;">금액</th>
                </tr>
              </thead>
              <tbody>
                {item_rows_html}
              </tbody>
            </table>
          </div>"""
            footer_html = f"""
          <div style="border-top: 1px solid var(--border-card); padding: 14px 20px; background: rgba(255, 255, 255, 0.03); display: flex; flex-direction: column; gap: 8px; font-size: 15px;">
            <div style="display: flex; justify-content: space-between; color: var(--text-muted);">
              <span>{spent_label}</span>
              <span style="font-weight: 700; color: var(--text-main);">{col['spent_krw']:,}원</span>
            </div>
            <div style="display: flex; justify-content: space-between; align-items: center;">
              <span style="color: var(--text-muted);">잔액</span>
              <span style="font-size: 19px; font-weight: 800; color: {col_remain_color};">{col['remain_krw']:,}원</span>
            </div>
          </div>"""

        expense_cols_html += f"""
        <div class="card" style="padding: 0; overflow: hidden; display: flex; flex-direction: column;">
          <div class="panel-header">
            <div>
              <div class="panel-title" style="display: flex; align-items: center; gap: 8px;">{col['display_name']} {purpose_badge_html}</div>
              {target_badge_html}
            </div>
            <span style="font-size: 18px; font-weight: 800; color: var(--emerald);">{col['div_krw']:,}원</span>
          </div>
          {body_html}
          {footer_html}
        </div>"""

    now_str = now.strftime("%Y년 %m월 %d일 %H:%M 기준")

    # 참고 및 주요 메모 (Slide 4) 및 추가 투자 시뮬레이션 (Slide 5)
    notes_entry = load_notes(target_month)
    note_items = notes_entry.get('items', []) if notes_entry else []
    sim_page = notes_entry.get('sim_page') if notes_entry else None
    has_notes_slide = isinstance(note_items, list) and len(note_items) > 0
    has_sim_slide = isinstance(sim_page, dict) and isinstance(sim_page.get('allocations'), dict)
    total_slides = 3 + (1 if has_notes_slide else 0) + (1 if has_sim_slide else 0)
    slide_3_next_btn = (
        '<button class="nav-btn" onclick="goToSlide(4)">참고 메모 보기 ▶</button>'
        if has_notes_slide else (
            '<button class="nav-btn" onclick="goToSlide(4)">추가 투자 시뮬레이션 ▶</button>'
            if has_sim_slide else ""
        )
    )

    slide_4_html = ""
    if has_notes_slide:
        notes_title = str(notes_entry.get('title') or '참고 및 주요 메모')
        notes_subtitle = str(notes_entry.get('subtitle') or f'{edition_title} 추가 참고 정보')
        emp_gross_annual_krw = round(sum(
            (s['quantity'] * monthly_dps * fx_rate) for s in overseas_stocks if s['broker'] == '미래에셋'
        ) * 12)
        dep_kb_annual_krw = round(sum(
            (s['quantity'] * monthly_dps * fx_rate) for s in overseas_stocks if s['broker'] == 'KB증권'
        ) * 12)
        dep_ki_annual_krw = round(sum(
            (s['quantity'] * monthly_dps * fx_rate) for s in overseas_stocks if s['broker'] == '한국투자'
        ) * 12)
        dep_gross_annual_krw = dep_kb_annual_krw + dep_ki_annual_krw

        compact = len(note_items) >= 3
        card_pad = "16px 22px" if compact else "22px 26px"
        li_mb = "6px" if compact else "10px"
        title_fs = "19px" if compact else "22px"
        hl_fs = "15.5px" if compact else "17.5px"
        hl_mb = "10px" if compact else "14px"

        note_cards_html = ""
        for idx, n_item in enumerate(note_items, 1):
            if not isinstance(n_item, dict):
                continue
            cat = str(n_item.get('category') or '메모')
            title = str(n_item.get('title') or '')
            highlight = str(n_item.get('highlight') or '')
            details = n_item.get('details') or []
            details_li = "".join(
                f'<li style="margin-bottom: {li_mb}; line-height: 1.5; color: var(--text-main);">{str(d)}</li>'
                for d in details
            )
            annual_ref_html = ""
            if n_item.get('footer_label') and n_item.get('footer_value'):
                tone = str(n_item.get('footer_tone') or 'cyan')
                tone_map = {
                    'emerald': ("rgba(16, 185, 129, 0.08)", "rgba(16, 185, 129, 0.25)", "var(--emerald)"),
                    'amber': ("rgba(245, 158, 11, 0.08)", "rgba(245, 158, 11, 0.28)", "var(--amber)"),
                    'rose': ("rgba(244, 63, 94, 0.08)", "rgba(244, 63, 94, 0.25)", "var(--rose)"),
                    'cyan': ("rgba(56, 189, 248, 0.08)", "rgba(56, 189, 248, 0.25)", "var(--cyan)"),
                }
                box_bg, box_bd, val_color = tone_map.get(tone, tone_map['cyan'])
                annual_ref_html = f"""
                <div style="margin-top: 14px; padding: 14px 16px; border-radius: 8px; background: {box_bg}; border: 1px solid {box_bd}; display: flex; justify-content: space-between; align-items: center; gap: 12px; font-size: 15px;">
                  <span style="color: var(--text-muted);">{str(n_item['footer_label'])}</span>
                  <span style="font-size: 16.5px; font-weight: 800; color: {val_color};">{str(n_item['footer_value'])}</span>
                </div>"""
            elif n_item.get('show_annual_div_ref'):
                emp_headroom = 20_000_000 - emp_gross_annual_krw
                if emp_headroom >= 0:
                    emp_status = f'한도 여유 {emp_headroom:,}원 · 추가 건보료 0원'
                    box_bg, box_bd, val_color = "rgba(16, 185, 129, 0.08)", "rgba(16, 185, 129, 0.25)", "var(--emerald)"
                else:
                    emp_add_monthly = round(((-emp_headroom) / 12) * 0.0719 * 1.1314)
                    emp_status = f'초과 +{-emp_headroom:,}원 · 월 약 {emp_add_monthly:,}원'
                    box_bg, box_bd, val_color = "rgba(244, 63, 94, 0.08)", "rgba(244, 63, 94, 0.25)", "var(--rose)"
                annual_ref_html = f"""
                <div style="margin-top: 14px; padding: 14px 16px; border-radius: 8px; background: {box_bg}; border: 1px solid {box_bd}; display: flex; flex-direction: column; gap: 4px; font-size: 15px;">
                  <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="color: var(--text-muted);">미래에셋증권 세전 연 배당(×12)</span>
                    <span style="font-weight: 800; color: {val_color};">{emp_gross_annual_krw:,}원 / 년</span>
                  </div>
                  <div style="text-align: right; font-size: 13.5px; font-weight: 600; color: var(--text-muted);">{emp_status}</div>
                </div>"""
            elif n_item.get('show_regional_div_ref'):
                dep_headroom = 20_000_000 - dep_gross_annual_krw
                if dep_headroom >= 0:
                    dep_status = f'한도 여유 {dep_headroom:,}원 · 피부양자 유지 (0원)'
                    box_bg, box_bd, val_color = "rgba(16, 185, 129, 0.08)", "rgba(16, 185, 129, 0.25)", "var(--emerald)"
                else:
                    reg_inc_hi = (dep_gross_annual_krw / 12) * 0.0719
                    reg_prop_hi = 559 * 211.5
                    reg_total_monthly = round((reg_inc_hi + reg_prop_hi) * 1.1314)
                    dep_status = f'초과 탈락 예상 · 월 {reg_total_monthly:,}원 (1주택 45%)'
                    box_bg, box_bd, val_color = "rgba(244, 63, 94, 0.08)", "rgba(244, 63, 94, 0.25)", "var(--rose)"
                annual_ref_html = f"""
                <div style="margin-top: 14px; padding: 14px 16px; border-radius: 8px; background: {box_bg}; border: 1px solid {box_bd}; display: flex; flex-direction: column; gap: 4px; font-size: 15px;">
                  <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="color: var(--text-muted);">KB({dep_kb_annual_krw:,}원) + 한투({dep_ki_annual_krw:,}원)</span>
                    <span style="font-weight: 800; color: {val_color};">{dep_gross_annual_krw:,}원 / 년</span>
                  </div>
                  <div style="text-align: right; font-size: 13.5px; font-weight: 600; color: var(--text-muted);">{dep_status}</div>
                </div>"""
            highlight_html = (
                f'<div style="font-size: {hl_fs}; font-weight: 700; color: var(--amber); margin-bottom: {hl_mb};">{highlight}</div>'
                if highlight else ""
            )
            full_width_style = " grid-column: 1 / -1;" if n_item.get('full_width') else ""
            ul_fs = "14.5px" if compact else "15.5px"
            note_cards_html += f"""
            <div class="card" style="display: flex; flex-direction: column; justify-content: space-between; padding: {card_pad};{full_width_style}">
              <div>
                <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 10px;">
                  <span style="font-size: 13px; font-weight: 700; color: var(--cyan); background: rgba(56, 189, 248, 0.12); border: 1px solid rgba(56, 189, 248, 0.3); padding: 4px 11px; border-radius: 999px;">{cat}</span>
                  <span style="font-size: 14px; color: var(--text-dim);">#{idx}</span>
                </div>
                <div style="font-size: {title_fs}; font-weight: 800; color: #fff; margin-bottom: 8px;">{title}</div>
                {highlight_html}
                <ul style="padding-left: 20px; font-size: {ul_fs}; color: var(--text-muted);">
                  {details_li}
                </ul>
              </div>
              {annual_ref_html}
            </div>"""

        notes_grid_class = "grid-2" if len(note_items) >= 2 else ""
        notes_grid_style = (
            "flex: 1; min-height: 0; overflow-y: auto; align-content: start; margin-bottom: 16px;"
            if len(note_items) >= 2
            else "flex: 1; min-height: 0; display: flex; flex-direction: column; justify-content: center; gap: 16px; margin-bottom: 16px;"
        )
    slide_4_next_btn = (
        '<button class="nav-btn" onclick="goToSlide(5)">추가 투자 시뮬레이션 ▶</button>'
        if has_sim_slide else ""
    )
    if has_notes_slide:
        slide_4_html = f"""
  <!-- SLIDE 4: 참고 및 주요 메모 ({edition_title}) -->
  <div class="slide" id="slide-4">
    <div>
      <div class="slide-header">
        <div>
          <div class="slide-title">{notes_title}</div>
          <div class="slide-meta" style="margin-top: 4px;">{notes_subtitle}</div>
        </div>
        <div class="slide-meta" style="text-align: right;">REFERENCE NOTES • {edition_title}<br>{now_str}</div>
      </div>
    </div>

    <div class="{notes_grid_class}" style="{notes_grid_style}">
      {note_cards_html}
    </div>

    <div class="deck-nav no-print">
      <div>슬라이드 4 / {total_slides} (방향키 ← / → 로 전환)</div>
      <div>
        <button class="nav-btn" onclick="goToSlide(3)">◀ 지출 현황 보기</button>
        {slide_4_next_btn}
      </div>
    </div>
  </div>"""

    slide_5_html = ""
    if has_sim_slide:
        sim_slide_num = 5 if has_notes_slide else 4
        sim_prev_num = sim_slide_num - 1
        sim_prev_label = "◀ 참고 메모 보기" if has_notes_slide else "◀ 지출 현황 보기"
        holder_map = {"KB증권": "피부양자", "한국투자": "피부양자", "미래에셋": "직장가입자"}
        unit_krw = agnc_live_price * fx_rate

        def _calc_sim(alloc_map):
            all_rows = []
            for db_broker, display_name in EXPENSE_BROKER_ORDER:
                old_qty = sum(s['quantity'] for s in overseas_stocks if s['broker'] == db_broker)
                add_krw = round(float(alloc_map.get(db_broker, 0) or 0))
                add_qty = int(add_krw / unit_krw) if unit_krw > 0 else 0
                new_qty = old_qty + add_qty
                old_net_m = round(old_qty * monthly_dps * (1 - tax_rate) * fx_rate)
                add_net_m = round(add_qty * monthly_dps * (1 - tax_rate) * fx_rate)
                new_net_m = old_net_m + add_net_m
                new_gross_y = round(new_qty * monthly_dps * 12 * fx_rate)
                all_rows.append({
                    'db_broker': db_broker,
                    'display_name': display_name,
                    'holder': holder_map[db_broker],
                    'add_krw': add_krw,
                    'old_qty': old_qty,
                    'add_qty': add_qty,
                    'new_qty': new_qty,
                    'old_net_m': old_net_m,
                    'add_net_m': add_net_m,
                    'new_net_m': new_net_m,
                    'new_gross_y': new_gross_y,
                })
            # 3개 증권사 모두 표시 (추가 투자 없는 계좌 포함)
            rows = all_rows
            dep_gross_y = sum(r['new_gross_y'] for r in all_rows if r['holder'] == '피부양자')
            emp_gross_y = sum(r['new_gross_y'] for r in all_rows if r['holder'] == '직장가입자')
            dep_fee_m = (
                round(((dep_gross_y / 12) * 0.0719 + 559 * 211.5) * 1.1314)
                if dep_gross_y > 20_000_000 else 0
            )
            dep_fee_m_60 = (
                round(((dep_gross_y / 12) * 0.0719 + 659 * 211.5) * 1.1314)
                if dep_gross_y > 20_000_000 else 0
            )
            emp_fee_m = (
                round(((emp_gross_y - 20_000_000) / 12) * 0.0719 * 1.1314)
                if emp_gross_y > 20_000_000 else 0
            )
            return {
                'rows': rows,
                'tot_add_krw': sum(r['add_krw'] for r in rows),
                'tot_old_qty': sum(r['old_qty'] for r in rows),
                'tot_add_qty': sum(r['add_qty'] for r in rows),
                'tot_new_qty': sum(r['new_qty'] for r in rows),
                'tot_old_net_m': sum(r['old_net_m'] for r in rows),
                'tot_add_net_m': sum(r['add_net_m'] for r in rows),
                'tot_new_net_m': sum(r['new_net_m'] for r in rows),
                'tot_new_gross_y': sum(r['new_gross_y'] for r in rows),
                'all_new_net_m': sum(r['new_net_m'] for r in all_rows),
                'dep_gross_y': dep_gross_y,
                'emp_gross_y': emp_gross_y,
                'dep_fee_m': dep_fee_m,
                'dep_fee_m_60': dep_fee_m_60,
                'emp_fee_m': emp_fee_m,
                'tot_fee_m': dep_fee_m + emp_fee_m,
            }

        sim = _calc_sim(sim_page['allocations'])

        sim_title = str(sim_page.get('title') or '퇴직금 추가 투자 시뮬레이션')
        sim_alloc_sub = " · ".join(
            f"{r['display_name']} {r['add_krw']:,}원"
            for r in sim['rows'] if r['add_krw'] > 0
        )
        sim_spend_map = sim_page.get('spend_mapping') or {}
        sim_cards_html = ""
        for r in sim['rows']:
            has_add = r['add_krw'] > 0
            exp_meta = expense_meta_by_broker.get(r['db_broker'], {})
            sim_meta = sim_spend_map.get(r['db_broker'], {}) if isinstance(sim_spend_map, dict) else {}
            purpose_val = str(sim_meta.get('purpose') or exp_meta.get('purpose') or '').strip()
            sim_purpose_html = _purpose_badge(purpose_val)
            usual_spend_text = str(sim_meta.get('usual_spend') or '').strip()
            header_badge = (
                f'<span style="font-size: 16px; font-weight: 800; color: var(--cyan); white-space: nowrap;">{r["add_krw"]:,}원 추가 투자</span>'
                if has_add
                else '<span style="font-size: 15px; font-weight: 700; color: var(--text-dim); white-space: nowrap;">추가 투자 없음</span>'
            )
            add_amt_html = (
                f'<span style="font-weight: 700; color: var(--cyan);">{r["add_krw"]:,}원</span> <span style="font-size: 13.5px; color: var(--text-dim); font-weight: 500;">(+{r["add_qty"]:,}주)</span>'
                if has_add
                else '<span style="color: var(--text-dim); font-weight: 600;">0원 (유지)</span>'
            )
            add_div_html = (
                f'<span style="font-weight: 700; color: var(--cyan);">+{r["add_net_m"]:,}원</span>'
                if has_add
                else '<span style="color: var(--text-dim); font-weight: 600;">0원</span>'
            )
            usual_row_html = (
                f"""<tr>
                  <td style="color: var(--text-muted); padding: 10px 14px;">예상 지출</td>
                  <td style="text-align: right; font-weight: 700; color: var(--amber); white-space: nowrap; padding: 10px 14px;">{usual_spend_text}</td>
                </tr>"""
                if usual_spend_text else ""
            )
            sim_cards_html += f"""
        <div class="card" style="padding: 0; overflow: hidden; display: flex; flex-direction: column;">
          <div class="panel-header">
            <div class="panel-title" style="display: flex; align-items: center; gap: 8px;">{r['display_name']} {sim_purpose_html}</div>
            {header_badge}
          </div>
          <div style="flex: 1; display: flex; flex-direction: column; justify-content: center;">
            <table>
              <tbody>
                <tr>
                  <td style="color: var(--text-muted); padding: 10px 14px;">추가 투자 금액</td>
                  <td style="text-align: right; white-space: nowrap; padding: 10px 14px;">{add_amt_html}</td>
                </tr>
                <tr>
                  <td style="color: var(--text-muted); padding: 10px 14px;">기존 수량 / 월 배당</td>
                  <td style="text-align: right; white-space: nowrap; padding: 10px 14px;">{r['old_qty']:,.0f}주 · {r['old_net_m']:,}원</td>
                </tr>
                <tr>
                  <td style="color: var(--text-muted); padding: 10px 14px;">추가 예상 월 배당</td>
                  <td style="text-align: right; white-space: nowrap; padding: 10px 14px;">{add_div_html}</td>
                </tr>
                <tr>
                  <td style="color: var(--text-muted); padding: 10px 14px;">투자 후 보유 수량</td>
                  <td style="text-align: right; font-weight: 700; white-space: nowrap; padding: 10px 14px;">{r['new_qty']:,.0f}주</td>
                </tr>
                {usual_row_html}
              </tbody>
            </table>
          </div>
          <div style="border-top: 1px solid var(--border-card); padding: 14px 18px; background: rgba(16, 185, 129, 0.06); display: flex; flex-direction: column; gap: 8px; font-size: 14.5px;">
            <div style="display: flex; justify-content: space-between; color: var(--text-muted);">
              <span>예상 연 배당 (세후 / 세전)</span>
              <span style="font-weight: 600; color: var(--text-main); white-space: nowrap;">{r['new_net_m'] * 12:,}원 / {r['new_gross_y']:,}원</span>
            </div>
            <div style="display: flex; justify-content: space-between; align-items: center;">
              <span style="color: var(--text-muted); font-weight: 700;">예상 월 배당 (실수령)</span>
              <span style="font-size: 24px; font-weight: 800; color: var(--emerald); white-space: nowrap;">{r['new_net_m']:,}원</span>
            </div>
          </div>
        </div>"""

        slide_5_html = f"""
  <!-- SLIDE {sim_slide_num}: {sim_title} ({edition_title}) -->
  <div class="slide" id="slide-{sim_slide_num}">
    <div>
      <div class="slide-header">
        <div>
          <div class="slide-title">{sim_title}</div>
        </div>
        <div class="slide-meta" style="text-align: right;">INVESTMENT SIMULATION • {edition_title}<br>{now_str}</div>
      </div>

      <div class="grid-3" style="margin-bottom: 16px;">
        <div class="card">
          <div class="kpi-label">총 추가 투자금</div>
          <div class="kpi-value">{sim['tot_add_krw']:,}원</div>
          <div class="kpi-sub">{sim_alloc_sub}</div>
        </div>
        <div class="card">
          <div class="kpi-label">추가 월 배당 증가 (실수령)</div>
          <div class="kpi-value" style="color: var(--cyan);">+{sim['tot_add_net_m']:,}원</div>
          <div class="kpi-sub">+{sim['tot_add_qty']:,}주 추가 매수 (15% 세후)</div>
        </div>
        <div class="card card-highlight">
          <div class="kpi-label">투자 후 예상 월 배당 (3계좌 합계)</div>
          <div class="kpi-value" style="color: var(--emerald);">{sim['tot_new_net_m']:,}원</div>
          <div class="kpi-sub">기존 {sim['tot_old_net_m']:,}원 → 총 {sim['tot_new_qty']:,.0f}주 (KB·한투·미래)</div>
        </div>
      </div>
    </div>

    <div class="grid-3" style="flex: 1; min-height: 0; align-items: stretch; margin-bottom: 16px;">
      {sim_cards_html}
    </div>

    <div class="deck-nav no-print">
      <div>슬라이드 {sim_slide_num} / {total_slides} (방향키 ← / → 로 전환)</div>
      <div>
        <button class="nav-btn" onclick="goToSlide({sim_prev_num})">{sim_prev_label}</button>
      </div>
    </div>
  </div>"""

    html_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>보유 자산 및 이번 달 배당금 현황 ({edition_title})</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css">
<style>
:root {{
  --bg-main: #0a0e17;
  --bg-card: rgba(18, 24, 38, 0.85);
  --border-card: rgba(255, 255, 255, 0.08);
  --text-main: #f1f5f9;
  --text-muted: #94a3b8;
  --text-dim: #64748b;
  --emerald: #10b981;
  --cyan: #38bdf8;
  --amber: #f59e0b;
  --rose: #f43f5e;
}}

* {{
  box-sizing: border-box;
  margin: 0;
  padding: 0;
  font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, sans-serif;
}}

body {{
  background-color: var(--bg-main);
  color: var(--text-main);
  min-height: 100vh;
  display: flex;
  justify-content: center;
  align-items: center;
  padding: 20px;
}}

.deck-container {{
  width: 100%;
  max-width: 1460px;
  aspect-ratio: 16 / 9;
  max-height: 92vh;
  background: #0f1523;
  border: 1px solid var(--border-card);
  border-radius: 16px;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.8);
  position: relative;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}}

.slide {{
  display: none;
  width: 100%;
  height: 100%;
  padding: 32px 46px 24px 46px;
  flex-direction: column;
  justify-content: space-between;
  overflow-y: auto;
}}

.slide.active {{
  display: flex;
}}

.slide-header {{
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  border-bottom: 1px solid var(--border-card);
  padding-bottom: 14px;
  margin-bottom: 18px;
}}

.slide-title {{
  font-size: 32px;
  font-weight: 800;
  color: #fff;
}}

.slide-meta {{
  font-size: 15px;
  color: var(--text-muted);
}}

.grid-2 {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
  align-items: stretch;
}}

.grid-3 {{
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}}

.grid-4 {{
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
}}

.panel-header {{
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 14px 20px;
  border-bottom: 1px solid var(--border-card);
  background: rgba(255, 255, 255, 0.03);
}}

.panel-title {{
  font-size: 18px;
  font-weight: 700;
  color: #fff;
}}

.card {{
  background: var(--bg-card);
  border: 1px solid var(--border-card);
  border-radius: 10px;
  padding: 18px 22px;
}}

.card-highlight {{
  border-color: rgba(16, 185, 129, 0.4);
  background: rgba(16, 185, 129, 0.05);
}}

.kpi-label {{
  font-size: 14px;
  font-weight: 600;
  color: var(--text-muted);
  margin-bottom: 4px;
}}

.kpi-value {{
  font-size: 30px;
  font-weight: 800;
  color: #fff;
}}

.kpi-sub {{
  font-size: 14px;
  color: var(--text-dim);
  margin-top: 4px;
}}

table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 15px;
}}

th {{
  background: rgba(255, 255, 255, 0.04);
  color: var(--text-muted);
  text-align: left;
  padding: 11px 14px;
  font-size: 14px;
  font-weight: 600;
  border-bottom: 1px solid var(--border-card);
}}

td {{
  padding: 12px 14px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.04);
}}

.deck-nav {{
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-top: 12px;
  border-top: 1px solid var(--border-card);
  font-size: 14px;
  color: var(--text-dim);
}}

.nav-btn {{
  background: rgba(255, 255, 255, 0.08);
  border: 1px solid var(--border-card);
  color: #fff;
  padding: 8px 16px;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 600;
  cursor: pointer;
}}

.nav-btn:hover {{
  background: rgba(255, 255, 255, 0.15);
}}

@media print {{
  body {{ background: #0a0e17 !important; padding: 0 !important; }}
  .deck-container {{ width: 16in !important; height: auto !important; border: none !important; }}
  .slide {{ display: flex !important; width: 16in !important; height: 9in !important; page-break-after: always; padding: 0.6in !important; }}
  .no-print {{ display: none !important; }}
}}
</style>
</head>
<body>

<div class="deck-container">

  <!-- ==========================================
       SLIDE 1: 현재 주가 & 환율 기준 자산 잔액 현황
       ========================================== -->
  <div class="slide active" id="slide-1">
    <div>
      <div class="slide-header">
        <div>
          <div style="font-size:14px; color:var(--cyan); font-weight:700; margin-bottom:4px;">ASSET BALANCE • {edition_title}</div>
          <h1 class="slide-title">보유 자산 잔액 현황</h1>
        </div>
        <div class="slide-meta">{now_str} (적용 환율: 1$ = {fx_rate:,.1f}원)</div>
      </div>

      <!-- 상단 핵심 잔액 집계 카드 -->
      <div class="grid-3" style="margin-bottom: 20px;">
        <div class="card card-highlight">
          <div class="kpi-label">총 보유 자산 잔액</div>
          <div class="kpi-value" style="color:var(--emerald);">{grand_total_eval_krw:,.0f}원</div>
          <div class="kpi-sub">국내 {dom_ratio:.1f}% · 해외 {ovs_ratio:.1f}%</div>
        </div>
        <div class="card">
          <div class="kpi-label">국내주식 잔액</div>
          <div class="kpi-value" style="color:var(--amber);">{tot_dom_eval:,.0f}원</div>
          <div class="kpi-sub">{len(domestic_stocks)}종목 보유</div>
        </div>
        <div class="card">
          <div class="kpi-label">해외주식 잔액</div>
          <div class="kpi-value" style="color:var(--cyan);">{tot_ovs_eval_krw:,.0f}원</div>
          <div class="kpi-sub">${tot_ovs_eval_usd:,.2f} (3개 증권사 합산)</div>
        </div>
      </div>

      <!-- 좌(국내주식) / 우(해외주식) 2단 분할 테이블 (평가손익 없이 잔액만 표시) -->
      <div class="grid-2">
        <!-- 왼쪽: 국내주식 -->
        <div class="card" style="padding:0; overflow:hidden; display:flex; flex-direction:column;">
          <div class="panel-header">
            <div class="panel-title"><span style="color:var(--amber);">●</span> 국내주식</div>
            <div style="font-size:18px; font-weight:800; color:var(--amber);">{tot_dom_eval:,.0f}원</div>
          </div>
          <table style="flex:1;">
            <thead>
              <tr>
                <th>종목명</th>
                <th style="text-align:right;">보유수량</th>
                <th style="text-align:right;">현재가</th>
                <th style="text-align:right;">잔액</th>
              </tr>
            </thead>
            <tbody>
              {"".join([f'''
              <tr>
                <td><strong>{s['name']}</strong> <span style="font-size:13px; color:var(--text-dim);">({s['code']})</span></td>
                <td style="text-align:right;">{s['quantity']:,} 주</td>
                <td style="text-align:right;">{s['current_price']:,.0f}원</td>
                <td style="text-align:right; font-weight:700;">{s['eval_value']:,.0f}원</td>
              </tr>
              ''' for s in domestic_stocks])}
            </tbody>
            <tfoot>
              <tr style="background:rgba(255,255,255,0.03); font-weight:800;">
                <td>국내주식 합계</td>
                <td style="text-align:right;">{sum(s['quantity'] for s in domestic_stocks):,} 주</td>
                <td></td>
                <td style="text-align:right; color:var(--amber); font-size:17px;">{tot_dom_eval:,.0f}원</td>
              </tr>
            </tfoot>
          </table>
        </div>

        <!-- 오른쪽: 해외주식 -->
        <div class="card" style="padding:0; overflow:hidden; display:flex; flex-direction:column;">
          <div class="panel-header">
            <div class="panel-title"><span style="color:var(--cyan);">●</span> 해외주식</div>
            <div style="font-size:18px; font-weight:800; color:var(--cyan);">{tot_ovs_eval_krw:,.0f}원</div>
          </div>
          <table style="flex:1;">
            <thead>
              <tr>
                <th>종목 (증권사)</th>
                <th style="text-align:right;">보유수량</th>
                <th style="text-align:right;">현재가</th>
                <th style="text-align:right;">잔액 (KRW / USD)</th>
              </tr>
            </thead>
            <tbody>
              {"".join([f'''
              <tr>
                <td><strong>{s['name']}</strong><div style="font-size:13px; color:var(--text-dim);">{s['broker']}</div></td>
                <td style="text-align:right;">{s['quantity']:,.0f} 주</td>
                <td style="text-align:right;">${s['current_price']:.2f}</td>
                <td style="text-align:right; font-weight:700;">
                  {s['eval_krw']:,.0f}원
                  <div style="font-size:13px; color:var(--text-dim); font-weight:500;">${s['eval_usd']:,.2f}</div>
                </td>
              </tr>
              ''' for s in overseas_stocks])}
            </tbody>
            <tfoot>
              <tr style="background:rgba(255,255,255,0.03); font-weight:800;">
                <td>해외주식 합계</td>
                <td style="text-align:right;">{tot_ovs_qty:,.0f} 주</td>
                <td></td>
                <td style="text-align:right; color:var(--cyan); font-size:17px;">
                  {tot_ovs_eval_krw:,.0f}원
                  <div style="font-size:13px; color:var(--text-dim); font-weight:500;">${tot_ovs_eval_usd:,.2f}</div>
                </td>
              </tr>
            </tfoot>
          </table>
        </div>
      </div>
    </div>

    <div class="deck-nav no-print">
      <div>슬라이드 1 / {total_slides} (방향키 ← / → 로 전환)</div>
      <div>
        <button class="nav-btn" onclick="goToSlide(2)">배당금 현황 보기 ▶</button>
      </div>
    </div>
  </div>

  <!-- ==========================================
       SLIDE 2: 이번 달 배당금 수령 현황
       ========================================== -->
  <div class="slide" id="slide-2">
    <div>
      <div class="slide-header">
        <div>
          <div style="font-size:14px; color:var(--emerald); font-weight:700; margin-bottom:4px;">MONTHLY DIVIDEND • {edition_title}</div>
          <h1 class="slide-title">이번 달 배당금 수령 현황</h1>
        </div>
        <div class="slide-meta">AGNC 인베스트먼트 (총 {tot_ovs_qty:,.0f} 주)</div>
      </div>

      <!-- 배당 핵심 KPI -->
      <div class="grid-2" style="margin-bottom: 20px;">
        <div class="card card-highlight">
          <div class="kpi-label">이번 달 실수령액 (세후 15% 공제)</div>
          <div class="kpi-value" style="color:var(--emerald);">{net_div_krw:,.0f}원</div>
          <div class="kpi-sub">${net_div_usd:,.2f} (통장 입금액)</div>
        </div>
        <div class="card">
          <div class="kpi-label">세전 배당 총액</div>
          <div class="kpi-value">{gross_div_krw:,.0f}원</div>
          <div class="kpi-sub">${gross_div_usd:,.2f} (주당 $0.12 · 3개 계좌 합산)</div>
        </div>
      </div>

      <!-- 증권사별 계좌 배당 입금 명세 -->
      <div class="card" style="padding:0; overflow:hidden;">
        <table>
          <thead>
            <tr>
              <th>증권사 (계좌)</th>
              <th style="text-align:right;">보유수량</th>
              <th style="text-align:right;">주당 배당금</th>
              <th style="text-align:right;">세전 배당금 (USD)</th>
              <th style="text-align:right;">실수령액 (USD, 15% 공제)</th>
              <th style="text-align:right;">실수령액 (원화 환산)</th>
              <th style="text-align:right;">비중</th>
            </tr>
          </thead>
          <tbody>
            {"".join([f'''
            <tr>
              <td><strong>{s['broker']}</strong></td>
              <td style="text-align:right;">{s['quantity']:,.0f} 주</td>
              <td style="text-align:right;">${monthly_dps:.2f}</td>
              <td style="text-align:right;">${(s['quantity'] * monthly_dps):,.2f}</td>
              <td style="text-align:right; font-weight:700;">${s['net_div_usd']:,.2f}</td>
              <td style="text-align:right; font-weight:700; color:var(--emerald);">{s['net_div_krw']:,.0f}원</td>
              <td style="text-align:right; color:var(--text-dim);">{s['quantity'] / tot_ovs_qty * 100:.1f}%</td>
            </tr>
            ''' for s in overseas_stocks])}
            <tr style="background:rgba(255,255,255,0.03); font-weight:800;">
              <td style="text-align:center;">합 계</td>
              <td style="text-align:right;">{tot_ovs_qty:,.0f} 주</td>
              <td style="text-align:right;">${monthly_dps:.2f}</td>
              <td style="text-align:right;">${gross_div_usd:,.2f}</td>
              <td style="text-align:right; color:var(--emerald);">${net_div_usd:,.2f}</td>
              <td style="text-align:right; color:var(--emerald); font-size:18px;">{net_div_krw:,.0f}원</td>
              <td style="text-align:right;">100.0%</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="deck-nav no-print">
      <div>슬라이드 2 / {total_slides} (방향키 ← / → 로 전환)</div>
      <div>
        <button class="nav-btn" onclick="goToSlide(1)">◀ 자산 현황 보기</button>
        <button class="nav-btn" onclick="goToSlide(3)">지출 현황 보기 ▶</button>
      </div>
    </div>
  </div>

  <!-- SLIDE 3: 지출 현황 (원화 전용) -->
  <div class="slide" id="slide-3">
    <div>
      <div class="slide-header">
        <div>
          <div class="slide-title">지출 현황</div>
          <div class="slide-meta" style="margin-top: 4px;">계좌별 이번 달 실수령 배당금 대비 지출 및 잔액 (원화 전용)</div>
        </div>
        <div class="slide-meta" style="text-align: right;">EXPENSE STATUS • {edition_title}<br>{now_str}</div>
      </div>

      <div class="grid-3" style="margin-bottom: 16px;">
        <div class="card card-highlight">
          <div class="kpi-label">이번 달 배당금 (실수령 합계)</div>
          <div class="kpi-value" style="color: var(--emerald);">{tot_exp_div_krw:,}원</div>
          <div class="kpi-sub">KB증권 · 한국투자증권 · 미래에셋증권 합산</div>
        </div>
        <div class="card">
          <div class="kpi-label">지출 합계</div>
          <div class="kpi-value">{tot_exp_spent_krw:,}원</div>
          <div class="kpi-sub">등록된 지출 항목 합계 (expenses.json)</div>
        </div>
        <div class="card">
          <div class="kpi-label">잔액</div>
          <div class="kpi-value" style="color: {tot_exp_remain_color};">{tot_exp_remain_krw:,}원</div>
          <div class="kpi-sub">배당금 실수령액 − 지출 합계</div>
        </div>
      </div>
    </div>

    <div class="grid-3" style="flex: 1; min-height: 0; align-items: stretch; margin-bottom: 16px;">
      {expense_cols_html}
    </div>

    <div class="deck-nav no-print">
      <div>슬라이드 3 / {total_slides} (방향키 ← / → 로 전환)</div>
      <div>
        <button class="nav-btn" onclick="goToSlide(2)">◀ 배당금 현황 보기</button>
        {slide_3_next_btn}
      </div>
    </div>
  </div>
{slide_4_html}
{slide_5_html}

</div>

<script>
let currentSlide = 1;
const totalSlides = {total_slides};
function goToSlide(n) {{
  currentSlide = Math.max(1, Math.min(totalSlides, n));
  for (let i = 1; i <= totalSlides; i++) {{
    document.getElementById('slide-' + i).classList.toggle('active', currentSlide === i);
  }}
}}
window.addEventListener('keydown', (e) => {{
  if (e.key === 'ArrowRight' || e.key === ' ' || e.key === 'PageDown') {{
    goToSlide(currentSlide + 1);
  }} else if (e.key === 'ArrowLeft' || e.key === 'PageUp') {{
    goToSlide(currentSlide - 1);
  }}
}});
</script>
</body>
</html>
"""

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    clean_month_str = target_month.replace("-", "_")
    output_filename = f"asset_report_{clean_month_str}.html"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print("=" * 60)
    print(f"✅ [{edition_title}] 자산 및 이번 달 배당금 보고서 생성 완료")
    print(f"• 저장 파일: {output_path}")
    print(f"• 총 평가금액: ₩{grand_total_eval_krw:,.0f} (손익 {'+' if grand_total_profit_krw >= 0 else ''}₩{grand_total_profit_krw:,.0f} / {grand_total_profit_rate:.2f}%)")
    print(f"• 이번 달 배당금(실수령): ₩{net_div_krw:,.0f} (${net_div_usd:,.2f} / 입금일: {pay_date})")
    print("=" * 60)
    return output_path

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="자산 및 배당금 현황 보고서 생성기")
    parser.add_argument("--month", "-m", type=str, default=None, help="대상 월 (예: 2026-10)")
    args = parser.parse_args()
    run_monthly_report(args.month)
