FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka cassandra-driver

COPY ./stock_to_scylla.py .

CMD ["python", "stock_to_scylla.py"]
