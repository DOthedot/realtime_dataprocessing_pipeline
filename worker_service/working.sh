// to see if the stream processor is working 
docker logs -f stream-processor


// to see the data in the table 
docker exec scylladb cqlsh -e "SELECT symbol, price, ts FROM stock_pipeline.stock_trades LIMIT 10;"
