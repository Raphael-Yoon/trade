#!/bin/bash

# 1. 기존에 돌고 있는 trade 관련 Gunicorn 프로세스만 종료 (PID 파일 활용)
PID_FILE="/home/raphael/Dev/pythons/trade/coffee_house.pid"
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p $PID > /dev/null 2>&1; then
        echo "Stopping existing Coffee House Gunicorn process (PID: $PID)..."
        kill -9 $PID
    fi
    rm "$PID_FILE"
fi

# 혹시 모를 잔여 프로세스 정리 (패턴 매칭)
pkill -f "gunicorn.*trade:app"

# 2. 조금의 틈을 주어 포트가 확실히 풀리도록 대기
sleep 1

# 2.5 DB 마이그레이션 실행 (신규 테이블 및 컬럼 자동 반영)
echo "Running DB Migration..."
/home/raphael/Dev/pythons/.venv/bin/python db_init.py

# 3. 127.0.0.1:5000번 포트로 백그라운드 구동 (PID 파일 지정)
/home/raphael/Dev/pythons/.venv/bin/gunicorn --daemon --workers 2 --bind 127.0.0.1:5000 --pid "$PID_FILE" --access-logfile flask.log --error-logfile flask.log trade:app

# 4. 결과 출력
echo "------------------------------------------------"
echo "Gunicorn server started successfully in the background!"
echo "Logs are being written to flask.log"
echo "------------------------------------------------"