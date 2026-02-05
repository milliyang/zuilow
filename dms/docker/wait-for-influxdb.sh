#!/bin/bash
# Wait for InfluxDB to be ready

set -e

host="$1"
port="$2"
shift 2
cmd="$@"

echo "Waiting for InfluxDB at $host:$port..."

until python -c "import socket; s = socket.socket(); s.settimeout(1); s.connect(('$host', $port)); s.close()" 2>/dev/null; do
  echo "InfluxDB is unavailable - sleeping"
  sleep 1
done

echo "InfluxDB is up - executing command"
exec $cmd
