#!/usr/bin/env bash
set -e

# Install extra Python deps mounted in from the host
if [ -e "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  pip install --user -r /opt/airflow/requirements.txt
fi

# Initialise the metadata DB and seed an admin user (both are safe to re-run)
airflow db init
airflow users create \
  --username admin \
  --password admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com || true

airflow db upgrade

exec airflow webserver
