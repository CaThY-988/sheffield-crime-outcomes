#!/usr/bin/env bash
set -e

export PATH="/opt/airflow/.venv/bin:$PATH"

exec "$@"