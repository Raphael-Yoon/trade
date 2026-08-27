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
| `Report/` | 추천종목 선정 로직 정의서 및 실행 절차 문서 (구 `cowork/Report/`, 개발2팀 이관) |
| `results/` | 분석 결과 저장 폴더 |
| `docs_cache/` | 수집된 공시 데이터 캐시 |

## 4. 환경 관리 원칙 (Local)

- **보존 대상**: `results/`, `docs_cache/`
- **삭제 대상**: 작업 중 생성한 임시 `.py` 스크립트 및 디버깅용 로그 파일
