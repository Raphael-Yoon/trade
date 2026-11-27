# -*- coding: utf-8 -*-
"""
내일의 공략주 KOSPI / KOSDAQ 2대 시장 트랙 전체 실행 마스터 파이프라인
(1차: KOSPI 30 + KOSDAQ 30 = 60종목 선별 -> 2차: 뉴스/공시/수급 수집 -> 3·4차: 점수화 및 각 TOP 10 총 20선 적재)
사용법: python run_pipeline.py --source_file results/[엑셀파일명].xlsx
"""
import sys
import os
import argparse
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
VENV_PYTHON = PROJECT_ROOT.parent / '.venv' / 'bin' / 'python'
if not VENV_PYTHON.exists():
    VENV_PYTHON = PROJECT_ROOT.parent / '.venv' / 'Scripts' / 'python.exe'
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
    parser = argparse.ArgumentParser(description="내일의 공략주 KOSPI/KOSDAQ 2대 시장 트랙 통합 분석 파이프라인 마스터 실행기")
    parser.add_argument('--source_file', type=str, required=False, help="1차 선별 대상 엑셀 파일 경로 (예: results/kospi,kosdaq_all_xxx.xlsx)")
    parser.add_argument('--id', type=str, required=False, help="Google Drive Spreadsheet ID")
    args = parser.parse_args()

    if not args.source_file and not args.id:
        print("[오류] --source_file 또는 --id 중 최소 하나는 반드시 지정해야 합니다.")
        sys.exit(1)

    source_desc = ""
    cmd_step1 = [VENV_PYTHON, PROJECT_ROOT / 'filter_financial_top20.py']

    if args.id:
        source_desc = f"구글 드라이브 ID: {args.id}"
        cmd_step1.extend(['--id', args.id])
        cmd_step1.extend(['--source_file', args.source_file or 'drive_downloaded.xlsx'])
    else:
        excel_file = Path(args.source_file)
        if not excel_file.exists():
            # 상대경로 대비 체크
            excel_file = PROJECT_ROOT / args.source_file
            if not excel_file.exists():
                print(f"[오류] 지정한 엑셀 파일이 존재하지 않습니다: {args.source_file}")
                sys.exit(1)
        source_desc = str(excel_file.resolve())
        cmd_step1.extend(['--source_file', source_desc])

    print("====================================================================")
    print("      🚀 [내일의 공략주] KOSPI/KOSDAQ 통합 분석 파이프라인 시작")
    print(f"      - 분석 소스: {source_desc}")
    print("====================================================================")

    # 1단계: 정량 선별 (KOSPI 30종목 + KOSDAQ 30종목 = 총 60개 후보군 추출)
    print("\n[Step 1] 1차 재무 정량 필터링 및 시장별 30위 후보군(총 60종목) 선출 진행...")
    run_script(cmd_step1)

    # 2단계: 실시간 뉴스, 공시, 추세·수급 데이터 수집
    print("\n[Step 2] 선출 후보 60개 종목에 대한 실시간 뉴스·DART 공시·수급 통합 수집 진행...")
    run_script([VENV_PYTHON, PROJECT_ROOT / 'collect_top20_context.py'])

    # 3단계 & 4단계: 정성 분석 점수화 및 SQLite/MySQL DB & JSON 배포 적재
    print("\n[Step 3 & 4] 뉴스 심리·공시 가감점·추세/수급 보정 결합 후 KOSPI/KOSDAQ 각 TOP 10(총 20선) 선발·적재 진행...")
    run_script([VENV_PYTHON, PROJECT_ROOT / 'process_final_recommendations.py'])

    print("\n====================================================================")
    print("      🎉 전체 파이프라인 분석 및 DB/JSON 배포 파일 생성 완료!")
    print("      - 배포 파일: trade/results/sector_recommendations.json")
    print("      - 로컬 DB: trade/trade.db (tr_audit_recommendations 테이블)")
    print("====================================================================")

if __name__ == '__main__':
    main()
