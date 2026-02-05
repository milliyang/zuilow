FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create wait script inline
RUN echo '#!/bin/bash\n\
set -e\n\
host="$1"\n\
port="$2"\n\
shift 2\n\
cmd="$@"\n\
echo "Waiting for InfluxDB at $host:$port..."\n\
until python -c "import socket; s = socket.socket(); s.settimeout(1); s.connect((\"$host\", $port)); s.close()" 2>/dev/null; do\n\
  echo "InfluxDB is unavailable - sleeping"\n\
  sleep 1\n\
done\n\
echo "InfluxDB is up - executing command"\n\
exec $cmd' > /wait-for-influxdb.sh && chmod +x /wait-for-influxdb.sh

# Copy startup script first (before copying all files, to ensure it's included)
COPY docker/start_dms_service.sh /start_dms_service.sh
RUN chmod +x /start_dms_service.sh

# Copy application code to /app/dms
COPY . /app/dms

# Set working directory to dms
WORKDIR /app/dms

EXPOSE 11183

# Start DMS (script will handle InfluxDB waiting based on WAIT_FOR_INFLUXDB env var)
CMD ["/start_dms_service.sh"]
