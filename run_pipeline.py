# -*- coding: utf-8 -*-
"""
내일의 공략주 3대 트랙 (대형주, 가치주, 상승주) 전체 실행 마스터 파이프라인
사용법: python run_pipeline.py --source_file results/[엑셀파일명].xlsx
"""
import sys
import os
import argparse
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
VENV_PYTHON = PROJECT_ROOT.parent / '.venv' / 'Scripts' / 'python.exe'

# 만약 가상환경 python이 없으면 시스템 python 사용
if not VENV_PYTHON.exists():
    VENV_PYTHON = Path(sys.executable)

def run_script(cmd_list):
    print(f"\n[*] Running: {' '.join(str(c) for c in cmd_list)}")
    res = subprocess.run(cmd_list, capture_output=False, text=True, encoding='utf-8')
    if res.returncode != 0:
        print(f"[오류] 스크립트 실행 실패. 코드: {res.returncode}")
        sys.exit(res.returncode)

def main():
    sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description="내일의 공략주 3대 트랙 전체 분석 파이프라인 마스터 실행기")
    parser.add_argument('--source_file', type=str, required=True, help="1차 선별 대상 엑셀 파일 경로 (예: results/kospi,kosdaq_all_xxx.xlsx)")
    args = parser.parse_args()

    excel_file = Path(args.source_file)
    if not excel_file.exists():
        # 상대경로 대비 체크
        excel_file = PROJECT_ROOT / args.source_file
        if not excel_file.exists():
            print(f"[오류] 지정한 엑셀 파일이 존재하지 않습니다: {args.source_file}")
            sys.exit(1)

    print("====================================================================")
    print("      🚀 [내일의 공략주] 3대 트랙 통합 분석 파이프라인 시작")
    print(f"      - 대상 파일: {excel_file.resolve()}")
    print("====================================================================")

    # 1단계: 정량 선별 (대형주, 가치주, 상승주 후보군 20개씩 추출)
    print("\n[Step 1] 1차 재무 정량 필터링 및 20위 후보군 선출 진행...")
    run_script([VENV_PYTHON, PROJECT_ROOT / 'filter_financial_top20.py', '--source_file', excel_file.resolve()])

    # 2단계: 실시간 뉴스 및 공시 데이터 수집
    print("\n[Step 2] 선출 후보 60개 종목에 대한 실시간 뉴스 및 DART 공시 통합 수집 진행...")
    run_script([VENV_PYTHON, PROJECT_ROOT / 'collect_top20_context.py'])

    # 3단계 & 4단계: 정성 분석 요약 평가 및 SQLite DB & JSON 배포 적재
    print("\n[Step 3 & 4] 뉴스 심리/공시 가감점 결합 및 5일선/20일선 추세 필터 적용 후 최종 탑텐 선발/적재 진행...")
    run_script([VENV_PYTHON, PROJECT_ROOT / 'process_final_recommendations.py'])

    print("\n====================================================================")
    print("      🎉 전체 파이프라인 분석 및 DB/JSON 배포 파일 생성 완료!")
    print("      - 배포 파일: trade/results/sector_recommendations.json")
    print("      - 로컬 DB: trade/trade.db (tr_audit_recommendations 테이블)")
    print("====================================================================")

if __name__ == '__main__':
    main()
