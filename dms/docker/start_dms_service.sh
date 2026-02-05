#!/bin/bash
# Start DMS service
# If WAIT_FOR_INFLUXDB is set, wait for InfluxDB before starting.
# GUNICORN_WORKERS: default 1 (one process = one InfluxDB connection; scheduler runs once).

set -e

WORKERS="${GUNICORN_WORKERS:-1}"

if [ "${WAIT_FOR_INFLUXDB:-true}" = "true" ]; then
    # Wait for InfluxDB if specified
    INFLUXDB_HOST="${INFLUXDB_HOST:-influxdb}"
    INFLUXDB_PORT="${INFLUXDB_PORT:-8086}"
    /wait-for-influxdb.sh "$INFLUXDB_HOST" "$INFLUXDB_PORT" gunicorn \
        --bind 0.0.0.0:11183 \
        --worker-class sync \
        --workers "$WORKERS" \
        --timeout 30 \
        --log-level info \
        --access-logfile - \
        --error-logfile - \
        app:app
else
    # Start directly without waiting
    exec gunicorn \
        --bind 0.0.0.0:11183 \
        --worker-class sync \
        --workers "$WORKERS" \
        --timeout 30 \
        --log-level info \
        --access-logfile - \
        --error-logfile - \
        app:app
fi
