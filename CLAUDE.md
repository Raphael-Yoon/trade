# Jonathan's Coffee House — 기술 명세 (Technical Specs)

> [!IMPORTANT]
> 에이전트 페르소나, 직급 체계 및 전사 운영 규칙은 루트의 [CLAUDE.md](file:///c:/Pythons/CLAUDE.md) 및 [.agents/AGENTS.md](file:///c:/Pythons/.agents/AGENTS.md)를 준수한다. 본 파일은 프로젝트별 기술 명령어 및 파일 참조용으로만 활용한다.

## 1. 빌드 및 실행 명령어 (Build & Run)

- **메인 시스템 실행 (Gunicorn 재구동)**: `./coffee_house_start.sh`
- **최신화 후 재구동 (Pull & Restart)**: `./coffee_house_reset.sh`
- **메인 시스템 종료 (Gunicorn 중지)**: `./coffee_house_stop.sh`
- **데이터 수집 파이프라인**: `python data_collect.py`
- **DART 공시 일괄 수집**: `python collect_disclosures.py --days 30` (최근 N일간 시장 전체 공시 수집)
- **드라이브 백업 동기화**: `python drive_sync.py`
- **추천종목 선정 파이프라인** (STEP 1~4, 개발2팀 김희선·김도희 담당. 상세 절차는 [추천종목_선정_작업지침서.md](추천종목_선정_작업지침서.md) 참조):
  - `python pool_collect.py --source_file <파일명>` — Pool 구성
  - `python select_top_10.py` — 뉴스·공시 수집 → `pool_context.json`
  - `python audit_save.py sector_rankings.json --type sector` — DB 적재 + `results/sector_recommendations.json` 생성

> [!NOTE]
> 시스템 내 AI API(Gemini) 직접 호출 기능은 제거되었습니다. AI 분석은 앱 내부가 아닌 별도 프롬프트를 통해서만 수행합니다.

## 2. 테스트 및 검증 (Tests)

- **수집 데이터 유효성 검사**: `python data_collect.py --test`
- **주문 로직 안정성 테스트 (권보성 담당)**: `pytest tests/trading/`

## 3. 프로젝트 파일 참조 (Reference)

| 파일 | 설명 |
|------|------|
| `trade.py` | 메인 트레이딩 애플리케이션 |
| `collect_disclosures.py` | DART 일괄 공시 수집 모듈 (AI-Free) |
| `data_collect.py` | 시장 데이터 수집 파이프라인 |
| `drive_sync.py` | Google Drive 백업 동기화 |
| `pool_collect.py` | 추천종목 파이프라인 STEP 1 — Pool 구성 (구 `cowork/pool_collect.py`, 개발2팀 이관) |
| `select_top_10.py` | 추천종목 파이프라인 STEP 2 — 뉴스·공시 수집 (구 `cowork/select_top_10.py`, 개발2팀 이관) |
| `audit_save.py` | 추천종목 파이프라인 STEP 4 — DB 적재 (구 `cowork/audit_save.py`, 개발2팀 이관) |
| `pool_save.py` | Pool 수동 저장 유틸리티 (구 `cowork/pool_save.py`, 개발2팀 이관) |
| `pool_naver_data.py` | 추천종목 파이프라인 전용 네이버 크롤러 (구 `cowork/get_all_naver_data.py`). `get_all_naver_data.py`(trade.py용)와는 별개 모듈 |
| `overseas_service.py` | 해외 주식 시세/환율/배당금 수집 및 일일 오전 자동 갱신 스케줄러 |
| `static/css/overseas.css` | 해외 주식 및 배당금 관리 전용 CSS |
| `static/js/overseas.js` | 해외 주식/배당금 관리 클라이언트 로직 및 자동 연산 엔진 |
| `Report/` | 추천종목 선정 로직 정의서 및 실행 절차 문서 (구 `cowork/Report/`, 개발2팀 이관) |
| `results/` | 분석 결과 저장 폴더 |
| `docs_cache/` | 수집된 공시 데이터 캐시 |

## 4. 환경 관리 원칙 (Local)

- **보존 대상**: `results/`, `docs_cache/`, `trade.db`
- **삭제 대상**: 작업 중 생성한 임시 `.py` 스크립트 및 디버깅용 로그 파일

## 5. 데이터베이스 스키마 명세 (Database Schemas — 해외주식/배당 신설)

### 📌 `tr_overseas_stocks` (해외 주식 보유 현황)
동일 종목이라도 계좌/증권사별로 분리 관리할 수 있도록 `id`를 기본키로 채택하고 `broker` 컬럼을 지원합니다.
- `id` (INTEGER, PK, AUTOINCREMENT): 고유 식별자
- `symbol` (TEXT / VARCHAR(50)): 티커 심볼 (예: `AAPL.O`, `TSLA.O`, `NVDA.O`)
- `name` (TEXT / VARCHAR(255)): 종목 한글/영문명 (예: `애플`)
- `broker` (TEXT / VARCHAR(50)): 증권사/계좌 구분 (예: `키움증권`, `토스증권`, `미래에셋` 등, 기본값 `'기본'`)
- `quantity` (REAL / DOUBLE): 보유 수량 (소수점 지원)
- `purchase_price` (REAL / DOUBLE): 매입 단가 (USD)
- `close_price` (REAL / DOUBLE): 최근 종가 (USD)
- `price_change` (REAL / DOUBLE): 전일 대비 등락폭 (USD)
- `fluctuations_ratio` (REAL / DOUBLE): 등락률 (%)
- `dividend_per_share` (REAL / DOUBLE): 주당 배당금 (DPS, USD)
- `dividend_yield` (REAL / DOUBLE): 배당수익률 (%)
- `dividend_date` (TEXT / VARCHAR(50)): 최근 배당 지급일
- `ex_dividend_date` (TEXT / VARCHAR(50)): 최근 배당락일
- `exchange_rate` (REAL / DOUBLE): 적용 환율 (USD/KRW)
- `currency` (TEXT / VARCHAR(10)): 통화 (기본 `'USD'`)
- `price_date` (TEXT / VARCHAR(50)): 시세 기준 거래일시
- `updated_at` (TEXT / VARCHAR(50)): 최종 데이터 갱신 일시
- `notes` (TEXT): 계좌 메모

### 📌 `tr_dividend_history` (배당금 실수령 내역)
증권계좌로 실제 입금된 배당금을 기록하고 월별/연도별 누적 통계를 제공합니다.
- `id` (INTEGER, PK, AUTOINCREMENT): 고유 식별자
- `symbol` (TEXT / VARCHAR(50)): 종목 티커
- `name` (TEXT / VARCHAR(255)): 종목명
- `broker` (TEXT / VARCHAR(50)): 수령 증권사 계좌 (예: `키움증권`, `토스증권` 등)
- `market_type` (TEXT / VARCHAR(20)): 시장 구분 (`'OVERSEAS'`, `'DOMESTIC'`)
- `payment_date` (TEXT / VARCHAR(50)): 배당금 입금 일자 (YYYY-MM-DD)
- `amount_gross` (REAL / DOUBLE): 세전 배당금 (주당배당금 × 수량)
- `amount_net` (REAL / DOUBLE): 세후 실수령액 (미국 배당소득세 15% 공제)
- `currency` (TEXT / VARCHAR(10)): 통화 (`'USD'`, `'KRW'`)
- `exchange_rate` (REAL / DOUBLE): 수령일 적용 환율
- `amount_net_krw` (REAL / DOUBLE): 원화 환산 실수령액
- `notes` (TEXT): 배당 메모 (예: `'2026년 3분기 배당'`)
- `created_at` (TEXT / VARCHAR(50)): 등록 일시

### 📌 `tr_overseas_daily_history` (해외 주식 일별 스냅샷 히스토리)
국내주식 `tr_stock_daily_history`와 동일한 수준으로 매일 아침 시세 갱신 시점의 계좌별 스냅샷과 환율을 누적 기록합니다.
- `id` (INTEGER / INT, PK, AUTOINCREMENT): 고유 식별자
- `date` (TEXT / VARCHAR(50)): 기준 일자 (YYYY-MM-DD)
- `symbol` (TEXT / VARCHAR(50)): 티커 심볼
- `name` (TEXT / VARCHAR(255)): 종목명
- `broker` (TEXT / VARCHAR(50)): 보유 증권사
- `quantity` (REAL / DOUBLE): 보유 수량
- `purchase_price` (REAL / DOUBLE): 매입 단가 (USD)
- `close_price` (REAL / DOUBLE): 당일 종가 (USD)
- `price_change` (REAL / DOUBLE): 전일 대비 등락폭 (USD)
- `fluctuations_ratio` (REAL / DOUBLE): 등락률 (%)
- `exchange_rate` (REAL / DOUBLE): 당일 적용 환율 (USD/KRW)
- `currency` (TEXT / VARCHAR(10)): 통화 (`'USD'`)
- `day_profit_usd` (REAL / DOUBLE): 당일 평가손익 (USD, `price_change * quantity`)
- `day_profit_krw` (REAL / DOUBLE): 당일 평가손익 (KRW)
- `cumulative_profit_usd` (REAL / DOUBLE): 누적 평가손익 (USD, `(close_price - purchase_price) * quantity`)
- `cumulative_profit_krw` (REAL / DOUBLE): 누적 평가손익 (KRW)
- `cumulative_profit_rate` (REAL / DOUBLE): 누적 수익률 (%)
- `eval_usd` (REAL / DOUBLE): 평가금액 (USD)
- `eval_krw` (REAL / DOUBLE): 평가금액 (KRW)
- `cost_usd` (REAL / DOUBLE): 투자원금 (USD)
- `cost_krw` (REAL / DOUBLE): 투자원금 (KRW)
- `recorded_at` (TEXT / VARCHAR(50)): 스냅샷 기록 일시

## 6. 주요 기능 및 아키텍처 (해외주식 & 배당)
- **다중 증권사 분리 관리**: 동일 종목(symbol)이라도 증권사(키움, 미래에셋, 토스 등)별로 각각 다른 수량과 매입단가로 등록 가능.
- **종목별 통합 합산 요약 (Stock Summary)**: 복수 증권사에 분산 보유된 종목을 티커 기준으로 자동 통합하여 총 보유수량, 가중평균 매입단가, 총 평가금액, 총 손익률, 총 연배당금 및 증권사별 보유 비중을 상단 요약 테이블에 일목요연하게 표시.
- **누적 손익 및 당일 평가손익(Daily P&L) 동시 병기**: 매입가 기준의 전체 누적 손익과 오늘 하루 미국 시장 변동에 따른 당일 손익(`price_change * qty`)을 메트릭 카드 및 테이블에 동시 표기.
- **일별 평가 스냅샷 자동 축적**: 매일 오전 08:30 스케줄러 갱신 및 시세 새로고침 시 `tr_overseas_daily_history`에 일별 평가/환율 스냅샷을 자동 적재하여 향후 기간별 리포트 기반 확보.
- **원클릭 배당 수령 자동연산**: 종목 리스트에서 [💵 배당수령] 클릭 시 수량 및 DPS 기반으로 미국 배당소득세(15%) 자동 공제 및 환율 연동 원화 환산액 실시간 산출.
- **일일 자동 시세 갱신**: 매일 오전 08:30(KST) 백그라운드 스케줄러가 네이버 증권 API를 통해 전 종목 종가 및 환율 최신화.

