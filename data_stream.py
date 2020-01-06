"""Spark Streaming pipeline for ingesting SF Crime event data"""
import json
import logging

import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

schema = StructType(
    [
        StructField("crime_id", StringType(), True),
        StructField("original_crime_type_name", StringType(), True),
        StructField("report_date", StringType(), True),
        StructField("call_date", StringType(), True),
        StructField("offense_date", StringType(), True),
        StructField("call_time", StringType(), True),
        StructField("call_date_time", StringType(), True),
        StructField("disposition", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("agency_id", StringType(), True),
        StructField("address_type", StringType(), True),
        StructField("common_location", StringType(), True),
    ]
)


def run_spark_job(spark: SparkSession, bootstrap_server: str, topic_name: str) -> None:
    """Run Spark Streaming data pipeline"""
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 200)
        .load()
    )

    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df.select(
        psf.from_json(psf.col("value"), schema).alias("SERVICE")
    ).select("SERVICE.*")

    service_table.printSchema()

    distinct_table = service_table.select(
        psf.col("original_crime_type_name"),
        psf.to_timestamp(psf.col("call_date_time")).alias("call_date_time"),
        psf.col("address"),
        psf.col("disposition"),
    )

    distinct_table.printSchema()

    agg_df = (
        distinct_table.withWatermark("call_date_time", "60 minutes")
        .groupBy(
            psf.window(distinct_table.call_date_time, "10 minutes"),
            distinct_table.original_crime_type_name,
        )
        .count()
    )

    query = agg_df.writeStream.format("console").outputMode("complete").start()

    query.awaitTermination()

    radio_code_json_filepath = "data/radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    join_query = (
        agg_df.join(radio_code_df, "disposition")
        .writeStream.format("console")
        .queryName("join")
        .start()
    )

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("KafkaSparkStructuredStreaming")
        .getOrCreate()
    )

    logger.info("Spark started")

    run_spark_job(spark, "localhost:9092", "org.sanfrancisco.sfpd.servicecalls.v1")

    spark.stop()
