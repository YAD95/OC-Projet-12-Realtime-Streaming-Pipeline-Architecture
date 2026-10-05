"""
Job Spark Structured Streaming : lit le topic Redpanda alimenté par Debezium
(CDC sur la table Postgres `activities`) et écrit en continu dans une table
Delta Lake "bronze" (données brutes, un cast minimal des types).

Le message Debezium a la forme :
    {"schema": {...}, "payload": {"before": ..., "after": {...}, "op": "c", ...}}
On ne garde que payload.after (l'état de la ligne après le changement) et on
ignore les suppressions (op == "d") pour ce POC.

Usage (depuis l'intérieur du conteneur spark) :
    spark-submit --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        /opt/spark_jobs/streaming_bronze.py
"""

import argparse
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


def get_spark(app_name="sds-streaming-bronze"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def build_envelope_schema():
    after_schema = StructType(
        [
            StructField("id", LongType()),
            StructField("id_salarie", IntegerType()),
            StructField("date_debut", LongType()),  # microsecondes depuis epoch (Debezium MicroTimestamp)
            StructField("sport_type", StringType()),
            StructField("distance_m", IntegerType()),
            StructField("date_fin", LongType()),
            StructField("commentaire", StringType()),
        ]
    )
    payload_schema = StructType(
        [
            StructField("after", after_schema),
            StructField("op", StringType()),
        ]
    )
    return StructType([StructField("payload", payload_schema)])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092"))
    parser.add_argument("--topic", default=os.getenv("ACTIVITIES_TOPIC", "sds.public.activities"))
    parser.add_argument("--bronze-path", default="/opt/data/bronze/activities")
    parser.add_argument("--checkpoint-path", default="/opt/data/checkpoints/bronze_activities")
    args = parser.parse_args()

    print(f"Lecture du topic '{args.topic}' sur {args.bootstrap_servers}...")

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    envelope_schema = build_envelope_schema()

    parsed = raw.selectExpr("CAST(value AS STRING) as json_str").select(
        F.from_json("json_str", envelope_schema).alias("envelope")
    )

    activities = (
        parsed.select("envelope.payload.after.*", "envelope.payload.op")
        .filter(F.col("op") != "d")  # on ignore les suppressions pour ce POC
        .withColumn("date_debut", (F.col("date_debut") / 1000000).cast("timestamp"))
        .withColumn("date_fin", (F.col("date_fin") / 1000000).cast("timestamp"))
        .withColumn("ingested_at", F.current_timestamp())
        .drop("op")
    )

    query = (
        activities.writeStream.format("delta")
        .option("checkpointLocation", args.checkpoint_path)
        .outputMode("append")
        .start(args.bronze_path)
    )

    print(f"Écriture en continu vers {args.bronze_path} (checkpoint: {args.checkpoint_path})")
    query.awaitTermination()


if __name__ == "__main__":
    main()