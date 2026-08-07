#!/bin/bash

# 1. 프로젝트 폴더 위치로 이동
cd /home/raphael/Dev/pythons/trade

# 2. 깃허브 최신 코드 pull 시도
echo "Pulling latest code from origin master..."
if ! git pull origin master; then
    echo "ERROR: git pull failed (conflict or network error). Gunicorn will not be restarted." >&2
    exit 1
fi

# 3. 최신 코드 반영 후 서버 재시작
./coffee_house_start.sh
