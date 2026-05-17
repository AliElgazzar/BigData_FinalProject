FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_TOPIC=wikimedia-recent-change

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY wikimedia_producer.py .

CMD ["python", "-u", "wikimedia_producer.py"]
