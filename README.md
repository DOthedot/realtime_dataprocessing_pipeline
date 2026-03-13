# Realtime Stock Trade Pipeline

A production-grade realtime data pipeline that ingests synthetic stock trade events through a Kafka-compatible message broker, processes them with Apache Flink, and persists the results in a time-series optimised ScyllaDB cluster.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Apache Flink](https://img.shields.io/badge/Apache%20Flink-1.18-orange?logo=apache-flink)
![Redpanda](https://img.shields.io/badge/Redpanda-Kafka%20Compatible-red)
![ScyllaDB](https://img.shields.io/badge/ScyllaDB-2025.4-blueviolet)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                         pipeline-network (Docker bridge)                     │
│                                                                              │
│  ┌─────────────┐     ┌──────────────────────────────────┐                   │
│  │  Producers  │     │         Redpanda (Kafka)          │                   │
│  │─────────────│     │──────────────────────────────────│                   │
│  │ producer-   │────▶│  stock-tech     (3 partitions)   │                   │
│  │   tech      │     │  stock-consumer (3 partitions)   │                   │
│  │ producer-   │────▶│  stock-mixed    (3 partitions)   │                   │
│  │  consumer   │     │                                  │                   │
│  │ producer-   │────▶│  Console UI → localhost:8080     │                   │
│  │   mixed     │     └──────────────┬───────────────────┘                   │
│  └─────────────┘                    │                                        │
│                          ┌──────────┴──────────┐                            │
│                          │                     │                            │
│                   ┌──────▼──────┐     ┌────────▼────────┐                  │
│                   │ Apache Flink│     │  Worker Service  │                  │
│                   │ (PRIMARY)   │     │  (ADD-ON)        │                  │
│                   │─────────────│     │──────────────────│                  │
│                   │ jobmanager  │     │ 3 Python replicas│                  │
│                   │ taskmanager │     │ batch size = 500 │                  │
│                   │ UI → :8081  │     │ async writes     │                  │
│                   └──────┬──────┘     └────────┬─────────┘                  │
│                          └──────────┬──────────┘                            │
│                                     │                                        │
│                           ┌─────────▼─────────┐                            │
│                           │     ScyllaDB       │                            │
│                           │────────────────────│                            │
│                           │ stock_pipeline     │                            │
│                           │   .stock_trades    │                            │
│                           │                    │                            │
│                           │ CQL → :9042        │                            │
│                           └────────────────────┘                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

| Stage | Service | Detail |
| --- | --- | --- |
| **Produce** | `data_source_service/` | 3 containers generate random stock trades and publish to Redpanda |
| **Broker** | `kafka_service/` | Redpanda exposes 3 Kafka topics, 3 partitions each. Kafka key = symbol for partition affinity |
| **Process** | `flink_service/` | Flink job unions all 3 topic streams, tags records with topic name, sinks to ScyllaDB |
| **Store** | `data_sink_service/` | ScyllaDB stores records in a time-series schema partitioned by `(symbol, trade_date)` |

### Message Schema

Each Kafka record is a JSON object produced by the stock producers:

```json
{
  "symbol":     "AAPL",
  "price":      182.45,
  "volume":     3200,
  "change_pct": 0.1234,
  "timestamp":  "2026-03-13T10:00:00.123456+00:00"
}
```

---

## Services

### Core Pipeline

| Service | Directory | Technology | Ports |
| --- | --- | --- | --- |
| Message Broker | `kafka_service/` | Redpanda (Kafka-compatible) | `19092` (Kafka), `8080` (Console UI) |
| Stock Producers | `data_source_service/` | Python 3.11 + confluent-kafka | — |
| Stream Processor | `flink_service/` | Apache Flink 1.18 + PyFlink | `8081` (Flink UI) |
| Time-Series DB | `data_sink_service/` | ScyllaDB 2025.4 | `9042` (CQL) |

### Add-On: Worker Service

| Service | Directory | Technology | When to use |
| --- | --- | --- | --- |
| Worker Processor | `worker_service/` | Python 3.11 + confluent-kafka + cassandra-driver | Lightweight alternative to Flink — no JVM, no cluster overhead |

> **Note:** Run either Flink **or** the Worker Service, not both simultaneously. Both consume the same Kafka topics and write to the same ScyllaDB table.

#### Flink vs Worker Service

| | Apache Flink | Worker Service |
| --- | --- | --- |
| **Parallelism model** | Distributed operator graph, task slots | 3 independent Python replicas |
| **State management** | Stateful (checkpointing, exactly-once) | Stateless |
| **Fault tolerance** | Built-in | Restart policy only |
| **Operational overhead** | JobManager + TaskManager cluster | Single Docker service |
| **Best for** | Production, complex transformations | Development, simple ingest |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with Compose v2)
- Python 3.11+ with [`uv`](https://github.com/astral-sh/uv) *(only needed to run producers locally)*

---

## Quick Start

Services must start in order — `kafka_service` creates the shared `pipeline-network` that all other services join.

### Step 1 — Start Redpanda

```bash
cd kafka_service && docker compose up -d
```

Creates `pipeline-network`, starts Redpanda, and initialises the 3 topics.

- Redpanda Console: [http://localhost:8080](http://localhost:8080)

### Step 2 — Start ScyllaDB

```bash
cd ../data_sink_service && docker compose up -d
```

Wait ~60 seconds for ScyllaDB to be healthy, then apply the schema:

```bash
# Confirm ScyllaDB is ready
docker exec scylladb cqlsh -e "DESCRIBE KEYSPACES;"

# Apply schema
docker exec -i scylladb cqlsh < data_sink_service/init.cql
```

### Step 3 — Start Flink (Primary Processor)

```bash
cd ../flink_service && docker compose up -d --build
```

Builds the PyFlink image (downloads Kafka JAR, installs `cassandra-driver`), then starts the Flink cluster.

- Flink UI: [http://localhost:8081](http://localhost:8081)

Submit the stream processing job:

```bash
docker exec flink_service-flink-jobmanager-1 \
  flink run -py /opt/flink/usr_jobs/scylla_sink.py
```

### Step 4 — Start Stock Producers

```bash
cd ../data_source_service && docker compose up -d --build
```

Starts 3 producer containers:

| Container | Topic | Symbols | Rate |
| --- | --- | --- | --- |
| `producer-tech` | `stock-tech` | AAPL, GOOGL, MSFT, NVDA | 1 event/sec |
| `producer-consumer` | `stock-consumer` | AMZN, TSLA, NFLX, META | 1 event/sec |
| `producer-mixed` | `stock-mixed` | AAPL, TSLA, AMZN, NVDA, NFLX | 2 events/sec |

---

## Alternative: Worker Service (Add-On)

If you want a lightweight processor without running a Flink cluster, use the worker service instead of Step 3.

> Make sure Flink is **not** running before starting the worker service.

```bash
cd worker_service && docker compose up -d --build
```

Starts 3 Python replicas. Each subscribes to all 3 Kafka topics, buffers up to 500 records, and flushes to ScyllaDB using async parallel writes.

---

## Verify the Pipeline

```bash
# All containers running
docker ps --format "table {{.Names}}\t{{.Status}}"

# Records accumulating in ScyllaDB
docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM stock_pipeline.stock_trades;"

# Latest trades
docker exec scylladb cqlsh -e \
  "SELECT symbol, price, volume, ts FROM stock_pipeline.stock_trades LIMIT 10;"

# Kafka topic message counts
docker exec redpanda rpk topic describe stock-tech
```

---

## Configuration

### Producer (`data_source_service/docker-compose.yml`)

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BROKER` | `redpanda:9092` | Kafka broker address |
| `TOPIC` | `stock-trades` | Topic to publish to |
| `SYMBOLS` | `AAPL,GOOGL,...` | Comma-separated ticker list |
| `INTERVAL` | `1` | Seconds between events (`0` = max speed) |
| `DURATION` | unset | Run for N seconds then stop (unset = forever) |

Run a producer locally for testing:

```bash
KAFKA_BROKER=localhost:19092 DURATION=10 TOPIC=stock-tech \
  uv run data_source_service/stock_producer.py
```

### Flink Job (`flink_service/pyflink/usr_jobs/scylla_sink.py`)

| Constant | Value | Description |
| --- | --- | --- |
| `REDPANDA_HOST` | `redpanda:9092` | Kafka broker (internal) |
| `SCYLLA_HOST` | `scylladb` | ScyllaDB hostname |
| `SCYLLA_KEYSPACE` | `stock_pipeline` | Target keyspace |
| `TOPICS` | all 3 topics | Topics consumed by the Flink job |

### Worker Service (`worker_service/docker-compose.yml`)

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BROKER` | `redpanda:9092` | Kafka broker address |
| `TOPICS` | all 3 topics | Comma-separated list |
| `SCYLLA_HOST` | `scylladb` | ScyllaDB hostname |
| `BATCH_SIZE` | `500` | Records per ScyllaDB flush |
| `POLL_TIMEOUT` | `0.1` | Kafka poll timeout in seconds |

---

## ScyllaDB Schema

```sql
-- Keyspace
CREATE KEYSPACE IF NOT EXISTS stock_pipeline
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}
    AND durable_writes = true
    AND TABLETS = {'enabled': false};

-- Table
CREATE TABLE IF NOT EXISTS stock_trades (
    symbol      text,
    trade_date  date,
    ts          timestamp,
    price       double,
    volume      int,
    change_pct  double,
    topic       text,
    PRIMARY KEY ((symbol, trade_date), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'HOURS',
      'compaction_window_size': 1
  };
```

**Design decisions:**

- **Partition key `(symbol, trade_date)`** — bounds partition size to one trading day per symbol, preventing unbounded growth
- **Clustering key `ts DESC`** — latest trades returned first without sorting
- **TWCS compaction** — minimises write amplification for append-only time-series workloads

---

## Useful Commands

```bash
# List Kafka topics
docker exec redpanda rpk topic list

# Inspect a specific topic
docker exec redpanda rpk topic describe stock-tech

# Truncate ScyllaDB table (clear all data)
docker exec scylladb cqlsh -e "TRUNCATE stock_pipeline.stock_trades;"

# Flink job logs (taskmanager)
docker logs -f flink_service-flink-taskmanager-1

# Worker service logs
docker logs -f worker_service-stream-processor-1

# Stop all services (in reverse dependency order)
docker compose -f data_source_service/docker-compose.yml down
docker compose -f flink_service/docker-compose.yml down
docker compose -f worker_service/docker-compose.yml down
docker compose -f data_sink_service/docker-compose.yml down
docker compose -f kafka_service/docker-compose.yml down
```

---

## Project Structure

```text
realtime_dataprocessing_pipeline/
├── kafka_service/              # Redpanda broker + topic initialisation
│   └── docker-compose.yml
├── data_source_service/        # Synthetic stock trade producers
│   ├── stock_producer.py
│   ├── Dockerfile
│   └── docker-compose.yml
├── flink_service/              # PRIMARY stream processor (Apache Flink)
│   ├── docker-compose.yml
│   └── pyflink/
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── download_libs.sh
│       └── usr_jobs/
│           ├── scylla_sink.py  # Main Flink job
│           └── kafka_consumer.py
├── data_sink_service/          # ScyllaDB + schema
│   ├── init.cql
│   └── docker-compose.yml
└── worker_service/             # ADD-ON: lightweight Python processor
    ├── stock_to_scylla.py
    ├── Dockerfile
    └── docker-compose.yml
```

---

## Tech Stack

| Component | Technology | Version |
| --- | --- | --- |
| Message broker | Redpanda | latest |
| Stream processor | Apache Flink + PyFlink | 1.18.1 |
| Database | ScyllaDB | 2025.4.5 |
| Producer / Worker runtime | Python | 3.11 |
| Kafka client | confluent-kafka | ≥ 2.3.0 |
| Database driver | cassandra-driver | 3.29.2 |
| Flink Kafka connector | flink-sql-connector-kafka | 3.1.0-1.18 |
| Package manager | uv | latest |
| Container runtime | Docker + Compose v2 | — |
