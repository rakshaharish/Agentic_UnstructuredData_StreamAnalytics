import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

def start_spark_stream():
   # Dynamic absolute path calculation to stay cleanly inside your project root
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    landing_zone_path = os.path.join(base_dir, "historical_data_lake", "raw_landing_zone")
    checkpoint_path = os.path.join(base_dir, "spark_checkpoints", "landing")

    # to guarantee Ollama and Docker don't trigger context-switching deadlocks.
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("MultiModelLakeProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=C:/hadoop") \
        .config("spark.executor.extraJavaOptions", "-Dhadoop.home.dir=C:/hadoop") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "100s") \
        .getOrCreate()

    # Read multi-model streams from Kafka
    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "redis-tx-topic,neo4j-tx-topic,mongo-tx-topic") \
        .load()
        

    # Transform payload structures into standard raw landing columns
    landing_zone_df = raw_kafka_stream.select(
        col("topic").alias("source_topic"),
        col("value").cast("string").alias("raw_payload"),
        current_timestamp().alias("landed_at")
    )

    # DIRECT STORAGE WRITE: Native Parquet format streaming sink
    query = landing_zone_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("kafka.consumer.cache.enabled", "false") \
        .start(landing_zone_path)

    query.awaitTermination()


if __name__ == "__main__":
    start_spark_stream()