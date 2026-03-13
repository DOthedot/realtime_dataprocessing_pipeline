# 1. List all keyspaces
docker exec scylladb cqlsh -e "DESCRIBE KEYSPACES;"

# 2. Check the table exists
docker exec scylladb cqlsh -e "DESCRIBE TABLE stock_pipeline.stock_trades;"

# 3. See how many rows have been written
docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM stock_pipeline.stock_trades;"

# 4. See the actual data
docker exec scylladb cqlsh -e "SELECT * FROM stock_pipeline.stock_trades LIMIT 10;"
