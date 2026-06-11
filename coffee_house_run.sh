#!/bin/bash

# 1. 현재 옛날 코드로 돌고 있는 Flask 프로세스 완전히 강제 종료
pkill -9 -f trade.py

# 2. 내 프로젝트 폴더 위치로 이동
cd /home/raphael/Dev/pythons/trade

# 3. 가상환경 내부 파이썬 엔진을 지정하여 새 코드로 백그라운드 재구동!
nohup ../.venv/bin/python3 trade.py > flask.log 2>&1 &
