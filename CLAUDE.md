# Jonathan's Coffee House — 기술 명세 (Technical Specs)

> [!IMPORTANT]
> 에이전트 페르소나, 직급 체계 및 전사 운영 규칙은 루트의 [CLAUDE.md](file:///c:/Python/CLAUDE.md)를 준수한다. 본 파일은 프로젝트별 기술 명령어 및 파일 참조용으로만 활용한다.

## 1. 빌드 및 실행 명령어 (Build & Run)

- **메인 시스템 실행 (Gunicorn 재구동)**: `./coffee_house_run.sh`
- **메인 시스템 종료 (Gunicorn 중지)**: `./coffee_house_stop.sh`
- **데이터 수집 파이프라인**: `python data_collect.py`
- **DART 공시 일괄 수집**: `python collect_disclosures.py --days 30` (최근 N일간 시장 전체 공시 수집)
- **AI 종목 분석 실행**: `python ai_analysis.py`
- **드라이브 백업 동기화**: `python drive_sync.py`

## 2. 테스트 및 검증 (Tests)

- **수집 데이터 유효성 검사**: `python data_collect.py --test`
- **주문 로직 안정성 테스트 (권보성 담당)**: `pytest tests/trading/`
- **AI 분석 리포트 생성 검증**: `python ai_analysis.py --report`

## 3. 프로젝트 파일 참조 (Reference)

| 파일 | 설명 |
|------|------|
| `trade.py` | 메인 트레이딩 애플리케이션 |
| `collect_disclosures.py` | DART 일괄 공시 수집 모듈 (AI-Free) |
| `ai_analysis.py` | AI 기반 종목 분석 모듈 |
| `data_collect.py` | 시장 데이터 수집 파이프라인 |
| `drive_sync.py` | Google Drive 백업 동기화 |
| `results/` | 분석 결과 저장 폴더 |
| `docs_cache/` | 수집된 공시 데이터 캐시 |

## 4. 환경 관리 원칙 (Local)

- **보존 대상**: `results/`, `docs_cache/`
- **삭제 대상**: 작업 중 생성한 임시 `.py` 스크립트 및 디버깅용 로그 파일
