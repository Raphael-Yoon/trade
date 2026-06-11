#!/bin/bash

# Gunicorn 운영용 서버 프로세스 완전히 종료
pkill -9 -f gunicorn
echo "Gunicorn processes terminated."
