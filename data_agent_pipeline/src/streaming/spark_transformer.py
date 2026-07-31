from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

def start_spark_stream():
    spark = SparkSession.builder \
        .appName("MultiModelLandingStream") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()

    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "redis-tx-topic,neo4j-tx-topic,mongo-tx-topic") \
        .load()

    landing_zone_df = raw_kafka_stream.select(
        col("topic").alias("source_topic"),
        col("value").cast("string").alias("raw_payload"),
        current_timestamp().alias("landed_at")
    )

    def write_to_db(batch_df, batch_id):
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:sqlite:historical_warehouse.db") \
            .option("dbtable", "raw_landing_zone") \
            .mode("append") \
            .save()

    query = landing_zone_df.writeStream \
        .foreachBatch(write_to_db) \
        .option("checkpointLocation", "./spark_checkpoints/landing") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    start_spark_stream()