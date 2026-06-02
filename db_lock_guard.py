# -*- coding: utf-8 -*-
import sys
import os
import psutil

def is_server_running():
    """
    5000번 포트 리스닝 여부 및 프로세스 목록 분석을 통해 trade.py Flask 서버 구동 상태 감지
    """
    # 1. 프로세스 커맨드라인 검사
    try:
        current_pid = os.getpid()
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            if proc.info['pid'] == current_pid:
                continue
            cmdline = proc.info['cmdline']
            if cmdline:
                cmd_str = " ".join(cmdline).lower()
                if "trade.py" in cmd_str and "python" in cmd_str:
                    return True
    except Exception:
        pass

    # 2. 5000번 포트 접속 점유 상태 검사
    try:
        for conn in psutil.net_connections(kind='inet'):
            if conn.laddr and conn.laddr.port == 5000:
                if conn.status == 'LISTEN':
                    return True
    except Exception:
        pass

    return False

def check_lock_and_exit(script_name):
    """
    서버 가동 여부를 검사하고 실행을 차단하거나 경고 출력
    """
    # --force-db 옵션이 파라미터에 있는지 검사
    force_run = False
    for arg in sys.argv:
        if arg == '--force-db':
            force_run = True
            break
            
    if is_server_running():
        if force_run:
            print("\n" + "="*80)
            print(f"⚠️  [경고] 현재 Flask 서버(trade.py)가 실행 중입니다.")
            print(f"⚠️  {script_name}이(가) 강제 옵션(--force-db)으로 가동됩니다. DB 락 충돌 리스크가 존재합니다.")
            print("="*80 + "\n")
        else:
            print("\n" + "⚠️"*40)
            print(f"⚠️  [서버 감지] 현재 Flask 서버(trade.py)가 실행 중입니다.")
            print(f"⚠️  동시 DB 작업 시 'Database Lock' 충돌이 발생할 수 있습니다.")
            print("⚠️"*40 + "\n")
            
            try:
                # 사용자에게 계속 진행할 것인지 확인
                choice = input("⚠️  작업 실행 중 DB 락 오류가 발생할 수 있습니다. 계속 진행하시겠습니까? (y/N): ").strip().lower()
                if choice not in ['y', 'yes']:
                    print("❌ 작업을 취소하고 종료합니다.")
                    sys.exit(1)
                else:
                    print("\n" + "="*80)
                    print(f"⚠️  {script_name}이(가) 사용자의 확인 하에 가동됩니다. DB 락 충돌 리스크가 존재합니다.")
                    print("="*80 + "\n")
            except (KeyboardInterrupt, EOFError):
                print("\n❌ 작업을 취소하고 종료합니다.")
                sys.exit(1)
