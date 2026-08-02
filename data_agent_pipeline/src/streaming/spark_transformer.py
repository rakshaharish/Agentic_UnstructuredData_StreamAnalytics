import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

STREAM_TOPICS = ["redis-tx-topic", "neo4j-tx-topic", "mongo-tx-topic"]

def ensure_topics_exist(bootstrap_servers):
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers.split(","),
        client_id="topic-ensurer"
    )
    try:
        existing = set(admin.list_topics())
        missing = [NewTopic(t, num_partitions=1, replication_factor=1)
                   for t in STREAM_TOPICS if t not in existing]
        if missing:
            try:
                admin.create_topics(new_topics=missing)
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()

def start_spark_stream():
    kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Make sure the subscribed topics exist before the consumer starts,
    # otherwise Spark fails with UnknownTopicOrPartitionException on first batch.
    ensure_topics_exist(kafka_bootstrap_servers)

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
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", ",".join(STREAM_TOPICS)) \
        .option("failOnDataLoss", "false") \
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