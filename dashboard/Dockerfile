FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc libsasl2-dev python3-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=100 -r requirements.txt

COPY dashboard.py .

EXPOSE 8501

CMD ["python", "dashboard.py"]
