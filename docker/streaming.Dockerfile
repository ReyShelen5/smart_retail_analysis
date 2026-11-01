# Image for the Kafka producer (POS simulator) and consumer (live monitor)
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PROJECT_DIR=/app \
    TZ=Asia/Kolkata

RUN apt-get update \
 && apt-get install -y --no-install-recommends tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka/ kafka/
RUN mkdir -p data/historical data/inventory data/streaming \
 && useradd -m -u 1000 appuser && chown -R appuser /app
USER appuser
