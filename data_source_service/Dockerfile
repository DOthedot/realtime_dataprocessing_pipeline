FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka fastavro

COPY stock_producer.py .

CMD ["python", "stock_producer.py"]
