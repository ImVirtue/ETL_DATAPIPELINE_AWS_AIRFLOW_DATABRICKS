#!/bin/bash
set -e

# Cài đặt các công cụ build và thư viện PostgreSQL
apt-get update && \
apt-get install -y --no-install-recommends \
gcc \
python3-dev \
libffi-dev \
libpq-dev && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

# Cài đặt các thư viện từ requirements-core.txt
if [ -e "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  pip install -r /opt/airflow/requirements.txt
fi

# Khởi tạo database và tạo user admin nếu chưa tồn tại
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# Nâng cấp database
airflow db upgrade

# Khởi động Airflow webserver
exec airflow webserver