"""Batch smoke test for task 2.2: count messages per CDC topic, as the streaming job would see them.

Run: docker compose -f docker-compose.laptop.yml --profile spark run --rm spark \
         /opt/spark/bin/spark-submit /opt/shopflow/smoke_kafka_count.py
"""

import os

from pyspark.sql import SparkSession

TABLES = ("customers", "products", "orders", "order_items", "inventory")
TOPICS = [f"cdc.public.{t}" for t in TABLES]


def main() -> None:
    spark = SparkSession.builder.appName("shopflow-smoke-kafka-count").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP"])
        .option("subscribe", ",".join(TOPICS))
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    for row in df.groupBy("topic").count().orderBy("topic").collect():
        print(f"SMOKE {row['topic']} {row['count']}")
    spark.stop()


if __name__ == "__main__":
    main()
