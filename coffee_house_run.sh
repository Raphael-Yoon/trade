#!/bin/bash

# 1. 현재 돌고 있는 Gunicorn 일꾼들을 완전히 종료
pkill -9 -f gunicorn

# 2. 프로젝트 폴더 위치로 이동
cd /home/raphael/Dev/pythons/trade

# 3. 파이썬 엔진을 통해 Gunicorn 운영용 서버를 새 코드로 백그라운드 재구동!
nohup /home/raphael/Dev/pythons/.venv/bin/python3 -m gunicorn --workers 2 --bind 0.0.0.0:5000 trade:app > flask.log 2>&1 &
