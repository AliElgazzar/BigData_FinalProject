import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
    current_timestamp,
    when,
    window,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or "kafka:9092"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC") or "wikimedia-recent-change"
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS") or "latest"

HDFS_BASE = "hdfs://namenode:9000/user/hive/warehouse/bigdata_project.db"

ENRICHED_PATH = f"{HDFS_BASE}/wikimedia_enriched_events"
WINDOW_PATH = f"{HDFS_BASE}/wikimedia_window_summary"
BOT_PATH = f"{HDFS_BASE}/wikimedia_bot_summary"

CHECKPOINT_BASE = "hdfs://namenode:9000/checkpoints/wikimedia_project"
REFERENCE_PATH = "hdfs://namenode:9000/data/wiki_reference.csv"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("WikimediaSparkStructuredStreaming")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.sql.parquet.writeLegacyFormat", "true")
        .getOrCreate()
    )


def load_reference(spark):
    return (
        spark.read
        .option("header", True)
        .csv(REFERENCE_PATH)
        .select(
            col("server_name"),
            col("language"),
            col("project_type"),
            col("region"),
        )
    )


def build_stream(spark, reference_df):
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("namespace", LongType(), True),
        StructField("editor_user", StringType(), True),
        StructField("bot", BooleanType(), True),
        StructField("server_name", StringType(), True),
        StructField("wiki", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("comment", StringType(), True),
    ])

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
        .filter(col("server_name").isNotNull())
        .filter(col("timestamp").isNotNull())
        .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp"))
    )

    enriched = (
        parsed.join(reference_df, on="server_name", how="left")
        .withColumn("language", when(col("language").isNull(), "Unknown").otherwise(col("language")))
        .withColumn("project_type", when(col("project_type").isNull(), "Unknown").otherwise(col("project_type")))
        .withColumn("region", when(col("region").isNull(), "Unknown").otherwise(col("region")))
        .select(
            col("id").cast("long"),
            col("type").cast("string"),
            col("title").cast("string"),
            col("namespace").cast("long"),
            col("editor_user").cast("string"),
            col("bot").cast("boolean"),
            col("server_name").cast("string"),
            col("wiki").cast("string"),
            col("event_time").cast("timestamp"),
            col("comment").cast("string"),
            col("language").cast("string"),
            col("project_type").cast("string"),
            col("region").cast("string"),
        )
    )

    return enriched


def write_all_outputs(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    batch_df.write.mode("append").parquet(ENRICHED_PATH)

    window_summary = (
        batch_df
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("server_name"),
            col("language"),
            col("project_type"),
        )
        .count()
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("server_name"),
            col("language"),
            col("project_type"),
            col("count").cast("long").alias("change_count"),
            current_timestamp().alias("processed_time"),
        )
    )
    window_summary.write.mode("append").parquet(WINDOW_PATH)

    bot_summary = (
        batch_df
        .withColumn("bot_label", when(col("bot") == True, "Bot").otherwise("Human"))
        .groupBy(
            col("server_name"),
            col("language"),
            col("project_type"),
            col("bot_label"),
        )
        .count()
        .select(
            col("server_name"),
            col("language"),
            col("project_type"),
            col("bot_label"),
            col("count").cast("long").alias("event_count"),
            current_timestamp().alias("processed_time"),
        )
    )
    bot_summary.write.mode("append").parquet(BOT_PATH)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    reference_df = load_reference(spark)
    stream_df = build_stream(spark, reference_df)

    query = (
        stream_df.writeStream
        .foreachBatch(write_all_outputs)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/main_writer")
        .trigger(processingTime="15 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

