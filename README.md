# Udacity Data Streaming Nanodegree: SF Crime Statistics with Spark Streaming

1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
   
   - **Answer** - It may affect number of processed micro-batches recived `processRowsPerSecond` but we can tune `maxOffsetPerTrigger` and `maxRatePerPartition`
2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

   - **Answer** - `spark.default.parallelism` and `spark.streaming.kafka.maxRatePerPartition` based on monitoring `processedRowsPerSecond`.
