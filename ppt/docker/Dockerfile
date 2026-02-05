FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc python3-dev libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install gunicorn eventlet>=0.24.1


# Copy project files
COPY . .

# Create directories and set permissions
RUN mkdir -p run/db run/logs run/opentimestamps && \
    chmod -R 777 run

# Command to run the application
CMD ["gunicorn", \
     "--bind", "0.0.0.0:11182", \
     "--worker-class", "eventlet", \
     "--workers", "1", \
     "--reload", \
     "--log-level", "debug", \
     "--access-logfile", "-", \
     "--error-logfile", "-", \
     "app:app"]