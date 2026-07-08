# 내일의 공략주 — Claude Code 실행 절차

> STEP 1 (Pool 구성)은 시스템 화면 버튼으로 실행.  
> **STEP 2~5는 이 문서를 따라 Claude Code에서 순서대로 실행.**

---

## 시작 — 사용할 Pool 선택

아래 명령을 Claude Code에 입력한다.

```
추천종목 선정 작업을 시작합니다. 현재 구성된 Pool 목록을 조회해줘
```

Claude가 `trade.db → tr_stock_pool`에서 구성된 Pool 목록(소스 파일명, 기준일, 종목 수)을 조회하여 보여준다.  
**어떤 Pool을 사용할지 확인 후 다음 단계를 진행한다.**

---

## STEP 2 — 뉴스·공시 수집

Pool이 확인되면 아래 명령을 Claude Code에 입력한다. (파일명은 앞 단계에서 선택한 것으로 교체)

```
python trade/select_top_10.py --source [선택한 source_file명] 실행해줘
```

완료 확인: `trade/pool_context.json` 파일이 생성되고 섹터별 종목 수가 출력되면 다음 단계로 진행.

---

## STEP 3 — 섹터별 순위 결정 (AI 평가)

아래 프롬프트를 Claude Code에 입력한다.

```
trade/pool_context.json 파일을 읽고, 각 섹터별 10개 종목의 투자 우선순위를 결정해 주세요.

[평가 지침]
- 각 섹터 내 10개 종목을 서로 비교하여 상대적 우위를 1~10위로 결정합니다.
- 뉴스(제목+본문)와 공시를 통해 현재 시장 모멘텀과 이슈를 판단합니다.
- 상승여력(upside)과 수급(supply)은 보조 참고 지표로 활용합니다.
- 재무 지표(roe, debt, pbr)는 섹터 내 안정성 비교에 활용합니다.
- 호재(수주, 실적 개선, 신제품)가 있는 종목을 우선합니다.
- 악재(횡령, 손실, 상장폐지 리스크)가 있는 종목은 후순위로 내립니다.
- **모든 종목에 one_liner를 반드시 작성합니다.** 재무 강점·시장 모멘텀·투자 포인트를 담은 1~2문장으로, 투자자가 한눈에 이 종목을 선택한 이유를 이해할 수 있어야 합니다.
  - 좋은 예: "자본 효율(ROE 55.1%) 대비 지주사 할인으로 순자산 매력도가 극대화된 가치투자처"
  - 나쁜 예: "좋은 종목입니다", "상승여력이 높습니다"

[출력 형식]
결과를 trade/sector_rankings.json 파일로 저장해 주세요.
pool_context.json의 재무 데이터를 그대로 포함하고, rank·reason·one_liner를 추가합니다.

{
  "섹터명": [
    {
      "rank": 1,
      "code": "종목코드",
      "name": "종목명",
      "sector": "섹터명",
      "current_price": 숫자,
      "target_price": 숫자,
      "upside": 숫자,
      "roe": 숫자,
      "debt": 숫자,
      "pbr": 숫자,
      "supply": "외인+/기관+",
      "data_date": "YYYY-MM-DD",
      "one_liner": "한 줄 투자 포인트",
      "reason": "선정 이유 (2~3문장, 뉴스·공시 근거 명시)",
      "news_summary": "pool_context.json의 news 배열을 JSON 문자열로 그대로 복사 (없으면 \"[]\")",
      "disc_json": "pool_context.json의 disclosures 배열을 JSON 문자열로 그대로 복사 (없으면 \"[]\")",
      "rec_type": "sector"
    }
  ]
}

섹터당 모든 10개 종목 포함, rank는 1~10 중복 없이 부여합니다.
```

완료 확인: `trade/sector_rankings.json` 파일이 생성되고 모든 섹터가 포함되어 있으면 다음 단계로 진행.

---

## STEP 4 — DB 적재 및 결과 JSON 생성

아래 명령을 Claude Code에 입력한다.

```
python trade/audit_save.py trade/sector_rankings.json --type sector 실행해줘
```

완료 확인:
- `trade.db → tr_audit_recommendations` 적재 성공 메시지 확인
- `trade/results/sector_recommendations.json` 파일 생성 확인

완료 후 임시 파일 삭제를 Claude Code에 요청한다.

```
trade/pool_context.json 과 trade/sector_rankings.json 삭제해줘
```

완료 확인: UI의 '내일의 공략주' 탭에서 섹터 탭과 종목 카드가 정상 표시되는지 확인.

---

## STEP 5 — 운영서버 마이그레이션 *(운영서버 배포 시에만 실행)*

운영서버의 `/api/targets/migrate`를 호출한다.  
`trade/results/sector_recommendations.json`이 운영서버에 있어야 한다.

> 로컬 서버에는 STEP 4에서 이미 DB 적재 완료. 로컬에서는 이 단계를 실행하지 않는다.
