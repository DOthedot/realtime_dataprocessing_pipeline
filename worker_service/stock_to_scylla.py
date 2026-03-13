import json
import logging
import os
import time
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPICS        = os.getenv("TOPICS", "stock-tech,stock-consumer,stock-mixed").split(",")
SCYLLA_HOST   = os.getenv("SCYLLA_HOST", "scylladb")
KEYSPACE      = "stock_pipeline"
BATCH_SIZE    = int(os.getenv("BATCH_SIZE", "100"))   # flush every N messages
POLL_TIMEOUT  = float(os.getenv("POLL_TIMEOUT", "0.1"))  # seconds

INSERT_CQL = """
    INSERT INTO stock_trades (symbol, trade_date, ts, price, volume, change_pct, topic)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""


def _make_cluster(host: str) -> Cluster:
    return Cluster(
        contact_points=[host],
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        protocol_version=4,
    )


def wait_for_scylla(host: str, retries: int = 20, delay: int = 5) -> Cluster:
    for attempt in range(1, retries + 1):
        try:
            cluster = _make_cluster(host)
            cluster.connect()
            log.info("ScyllaDB ready.")
            return cluster
        except Exception as e:
            log.info("ScyllaDB not ready (attempt %d/%d): %s", attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError("ScyllaDB unreachable after retries.")


def flush(session, stmt, rows: list) -> None:
    futures = [session.execute_async(stmt, row) for row in rows]
    for f in futures:
        f.result()  # wait for all async writes to complete


def main():
    cluster = wait_for_scylla(SCYLLA_HOST)
    session = cluster.connect(KEYSPACE)
    stmt = session.prepare(INSERT_CQL)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "scylla-sink",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(TOPICS)
    log.info("Consuming from %s → writing to ScyllaDB (batch=%d)", TOPICS, BATCH_SIZE)

    buffer = []

    try:
        while True:
            msg = consumer.poll(POLL_TIMEOUT)

            if msg is not None and not msg.error():
                record = json.loads(msg.value().decode("utf-8"))
                ts = datetime.fromisoformat(record["timestamp"])
                buffer.append((
                    record["symbol"],
                    ts.date(),
                    ts,
                    float(record["price"]),
                    int(record["volume"]),
                    float(record["change_pct"]),
                    msg.topic(),
                ))
            elif msg is not None:
                code = msg.error().code()
                if code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    log.warning("Topic not available yet, waiting for producers...")
                elif code != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())

            # flush when batch is full or no more messages and buffer has something
            if len(buffer) >= BATCH_SIZE or (msg is None and buffer):
                flush(session, stmt, buffer)
                log.info("Flushed %d records to ScyllaDB", len(buffer))
                buffer.clear()

    except KeyboardInterrupt:
        if buffer:
            flush(session, stmt, buffer)
    finally:
        consumer.close()
        cluster.shutdown()


if __name__ == "__main__":
    main()
