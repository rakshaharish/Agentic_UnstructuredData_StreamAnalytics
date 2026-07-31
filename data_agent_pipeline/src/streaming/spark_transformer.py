import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

def start_spark_stream():
    # Dynamic absolute path calculation to stay cleanly inside your project root
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    landing_zone_path = os.path.join(base_dir, "historical_data_lake", "raw_landing_zone")
    checkpoint_path = os.path.join(base_dir, "spark_checkpoints", "landing")

    # Initialize PySpark targeting Kafka 4.2.0
    # NOTE: If using Delta Lake, swap format("parquet") below for format("delta")
    spark = SparkSession.builder \
        .appName("MultiModelLakeProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
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

    # DIRECT STORAGE WRITE: Native Parquet or Delta Lake format streaming sink
    query = landing_zone_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .start(landing_zone_path)

    query.awaitTermination()


if __name__ == "__main__":
    start_spark_stream()