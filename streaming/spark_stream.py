"""Kafka to Delta Lake Structured Streaming job.

The stream preserves the Debezium envelope so schema changes do not require
restarting the ingestion job. Use --once for a bounded demo run; omit it for
continuous processing.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    get_json_object,
    lit,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka to Delta streaming Bronze")
    parser.add_argument("--kafka", default="kafka:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topics", default="dbserver1.inventory.customers,dbserver1.inventory.orders,dbserver1.inventory.products")
    parser.add_argument("--output", default="/opt/project/spark_bronze/events")
    parser.add_argument("--checkpoint", default="/opt/project/checkpoints/kafka_to_delta")
    parser.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    parser.add_argument("--once", action="store_true", help="Process available records and exit")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("cdc-kafka-to-delta-bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    topics = [topic.strip() for topic in args.topics.split(",") if topic.strip()]
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka)
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    events = (
        kafka_stream
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("key").cast("string").alias("record_key"),
            col("value").cast("string").alias("debezium_event"),
        )
        .where(col("debezium_event").isNotNull())
        .withColumn("table", get_json_object(col("debezium_event"), "$.payload.source.table"))
        .withColumn("operation", get_json_object(col("debezium_event"), "$.payload.op"))
        .withColumn("event_ts", get_json_object(col("debezium_event"), "$.payload.ts_ms").cast("long"))
        .withColumn("before_json", get_json_object(col("debezium_event"), "$.payload.before"))
        .withColumn("after_json", get_json_object(col("debezium_event"), "$.payload.after"))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("stream_version", lit("spark-structured-streaming"))
    )

    writer = (
        events.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint)
        .option("mergeSchema", "true")
        .partitionBy("table")
    )
    if args.once:
        writer = writer.trigger(availableNow=True)

    query = writer.start(args.output)
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
