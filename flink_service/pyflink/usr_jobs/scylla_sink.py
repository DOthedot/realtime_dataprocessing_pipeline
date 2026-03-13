import json
import logging
from datetime import datetime

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import MapFunction

REDPANDA_HOST = "redpanda:9092"
SCYLLA_HOST = "scylladb"
SCYLLA_KEYSPACE = "stock_pipeline"
TOPICS = ["stock-tech", "stock-consumer", "stock-mixed"]


class ScyllaSink(MapFunction):
    """Side-effecting MapFunction that writes each record to ScyllaDB.
    PyFlink's add_sink() only works with Java-backed sinks; using MapFunction
    is the correct pattern for custom Python sinks."""

    def __init__(self, contact_points: list[str], keyspace: str):
        self.contact_points = contact_points
        self.keyspace = keyspace
        # Non-serializable resources — initialized lazily on the task manager
        self._cluster = None
        self._session = None
        self._insert_stmt = None

    def map(self, value: str) -> str:
        if self._session is None:
            from cassandra.cluster import Cluster
            self._cluster = Cluster(self.contact_points)
            self._session = self._cluster.connect(self.keyspace)
            self._insert_stmt = self._session.prepare(
                "INSERT INTO stock_trades "
                "(symbol, trade_date, ts, price, volume, change_pct, topic) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            )

        data = json.loads(value)
        ts = datetime.fromisoformat(data["timestamp"])
        trade_date = ts.date()

        self._session.execute(
            self._insert_stmt,
            (
                data["symbol"],
                trade_date,
                ts,
                float(data["price"]),
                int(data["volume"]),
                float(data["change_pct"]),
                data["topic"],
            ),
        )
        return value


def configure_source(server: str, topic: str, earliest: bool = False) -> KafkaSource:
    properties = {
        "bootstrap.servers": server,
        "group.id": f"flink-{topic}",
    }
    offset = (
        KafkaOffsetsInitializer.earliest()
        if earliest
        else KafkaOffsetsInitializer.latest()
    )
    return (
        KafkaSource.builder()
        .set_topics(topic)
        .set_properties(properties)
        .set_starting_offsets(offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def add_topic_field(record: str, topic: str) -> str:
    data = json.loads(record)
    data["topic"] = topic
    return json.dumps(data)


def initialize_env() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    root_dir = "/".join(__file__.split("/")[:-2])
    env.add_jars(
        f"file://{root_dir}/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
    )
    return env


def main() -> None:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    logger.info("Initializing Flink environment")
    env = initialize_env()

    # Build one stream per topic, tag each record with its topic name, then union
    streams = []
    for topic in TOPICS:
        source = configure_source(REDPANDA_HOST, topic)
        stream = env.from_source(
            source, WatermarkStrategy.no_watermarks(), f"Redpanda {topic}"
        )
        tagged = stream.map(lambda x, t=topic: add_topic_field(x, t))
        streams.append(tagged)

    merged = streams[0].union(*streams[1:])

    scylla_sink = ScyllaSink(
        contact_points=[SCYLLA_HOST],
        keyspace=SCYLLA_KEYSPACE,
    )
    merged.map(scylla_sink)
    logger.info("Pipeline ready — executing")

    env.execute("Flink ScyllaDB Stock Trades Sink")


if __name__ == "__main__":
    main()
