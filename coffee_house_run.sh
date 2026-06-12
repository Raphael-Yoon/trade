#!/bin/bash

# 1. 프로젝트 폴더 위치로 이동
cd /home/raphael/Dev/pythons/trade

# 2. 깃허브 최신 코드 pull 시도 (실패 시 즉시 중단하여 기존 서버 보호)
if ! git pull origin master; then
    echo "ERROR: git pull failed (conflict or network error). Gunicorn will not be restarted." >&2
    exit 1
fi

# 3. pull 성공 시에만 기존 Gunicorn 프로세스를 강제 종료하고 재구동
pkill -9 -f gunicorn

echo "Starting Gunicorn server..."
nohup /home/raphael/Dev/pythons/.venv/bin/python3 -m gunicorn --workers 2 --bind 0.0.0.0:5000 trade:app > flask.log 2>&1 &

# 4. 2초 대기 후 프로세스가 정상 구동되었는지 검증
sleep 2
if pgrep -f gunicorn > /dev/null; then
    echo "------------------------------------------------"
    echo "Gunicorn server started successfully in the background!"
    echo "PID(s): $(pgrep -f gunicorn | tr '\n' ' ')"
    echo "Logs are being written to flask.log (use 'tail -f flask.log' to view)"
    echo "------------------------------------------------"
else
    echo "ERROR: Gunicorn failed to start. Please check flask.log for details." >&2
    exit 1
fi
