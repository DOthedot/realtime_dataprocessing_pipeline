CREATE KEYSPACE IF NOT EXISTS stock_pipeline
    WITH replication = {
        'class': 'NetworkTopologyStrategy',
        'replication_factor': 1
    }
    AND durable_writes = true
    AND TABLETS = {'enabled': false};

USE stock_pipeline;

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
  }
  AND gc_grace_seconds = 86400
  AND comment = 'Stock trade events ingested from Kafka via Flink';
