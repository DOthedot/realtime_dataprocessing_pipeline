# Realtime Data Processing Pipeline

A realtime stock trade pipeline that ingests high-throughput market data through Kafka into ScyllaDB.

```text
Stock Producers  →  Redpanda (Kafka)  →  Stream Processors  →  ScyllaDB
  (3 containers)     (3 topics)           (3 containers)        (stock_trades)
```

## Services

| Service | Folder | Description |
|---|---|---|
| Kafka (Redpanda) | `kafka_service/` | Message broker with 3 topics, 3 partitions each |
| Stock Producers | `data_source_servie/` | Generates random stock trade events |
| Stream Processor | `flink_service/` | Consumes Kafka, writes to ScyllaDB |
| ScyllaDB | `data_sink_service/` | Write-heavy time-series database |

## Prerequisites

- Docker Desktop
- Python 3.11+ with `uv`

## Start the Pipeline

### Step 1 — Kafka

```bash
cd kafka_service && docker compose up -d
```

Starts Redpanda, creates 3 topics with 3 partitions each (`stock-tech`, `stock-consumer`, `stock-mixed`), and the Redpanda Console UI at [http://localhost:8080](http://localhost:8080).

### Step 2 — ScyllaDB

```bash
cd data_sink_service && docker compose up -d
```

Wait until ScyllaDB is healthy (~60s), then apply the schema:

```bash
docker exec scylladb cqlsh -e "DESCRIBE KEYSPACES;"   # wait until this works
docker exec -i scylladb cqlsh < data_sink_service/init.cql
```

### Step 3 — Stream Processor

```bash
cd flink_service && docker compose up -d --build
```

Starts 3 consumer replicas. Each reads from Kafka and batch-writes to ScyllaDB.

### Step 4 — Stock Producers

```bash
cd data_source_servie && docker compose up -d --build
```

Starts 3 producer containers writing to different topics at full speed.

---

## Verify Data is Flowing

```bash
# Check all containers are running
docker ps --format "table {{.Names}}\t{{.Status}}"

# Watch records accumulate in ScyllaDB
docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM stock_pipeline.stock_trades;"

# View latest trades
docker exec scylladb cqlsh -e "SELECT symbol, price, ts FROM stock_pipeline.stock_trades LIMIT 10;"

# Stream processor logs
docker logs -f flink_service-stream-processor-1
```

---

## Configuration

### Producer env vars (`data_source_servie/docker-compose.yml`)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `redpanda:9092` | Kafka broker address |
| `TOPIC` | `stock-trades` | Topic to write to |
| `SYMBOLS` | all 8 tickers | Comma-separated list |
| `INTERVAL` | `0` | Seconds between events (0 = max speed) |
| `DURATION` | unset | Run for N seconds then stop (unset = forever) |

Run a producer manually for 10 seconds:

```bash
DURATION=10 TOPIC=stock-tech uv run data_source_servie/stock_producer.py
```

### Stream processor env vars (`flink_service/docker-compose.yml`)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `redpanda:9092` | Kafka broker address |
| `TOPICS` | all 3 topics | Comma-separated list |
| `SCYLLA_HOST` | `scylladb` | ScyllaDB hostname |
| `BATCH_SIZE` | `500` | Records per ScyllaDB flush |

---

## Useful Commands

```bash
# List Kafka topics and partition counts
docker exec redpanda rpk topic list

# Clear all data from ScyllaDB
docker exec scylladb cqlsh -e "TRUNCATE stock_pipeline.stock_trades;"

# Stop everything
docker compose -f kafka_service/docker-compose.yml down
docker compose -f data_sink_service/docker-compose.yml down
docker compose -f flink_service/docker-compose.yml down
docker compose -f data_source_servie/docker-compose.yml down
```

---

## Architecture Notes

- **Kafka key** = stock symbol → same symbol always goes to the same partition
- **ScyllaDB partition key** = `(symbol, trade_date)` → bounded partition size per day
- **Clustering key** = `ts DESC` → latest trades first
- **TWCS compaction** → optimized for time-series append-only writes
- **Async batch writes** → stream processor fires 500 writes in parallel per flush
