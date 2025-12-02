#!/usr/bin/env python3
"""
Structured Streaming job for the loan fact.

Reads loan events from a Kafka topic, lands them in a silver fact table,
performs simple clean-up and lookups, and then loads a gold Iceberg
fact table. Transformations and writes are expressed using Spark SQL.

Example:
  spark-submit scripts/etl/fact_loan_streaming.py \\
    --bootstrap-servers localhost:9092 \\
    --loan-topic berka_loans \\
    --silver-db silver \\
    --silver-table fact_loan_silver \\
    --gold-db gold \\
    --gold-table fact_loan \\
    --checkpoint-location /tmp/berka_fact_loan_chk \\
    --trigger-seconds 30
"""

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Structured Streaming loan fact job (Kafka → Silver → Gold Iceberg)."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=(
            "kafka-demo-corebroker0.maybank1.xfaz-gdb4.cloudera.site:9093,"
            "kafka-demo-corebroker1.maybank1.xfaz-gdb4.cloudera.site:9093,"
            "kafka-demo-corebroker2.maybank1.xfaz-gdb4.cloudera.site:9093"
        ),
        help=(
            "Kafka bootstrap servers (host:port, comma separated). "
            "Default matches the Berka data generator cluster."
        ),
    )
    parser.add_argument(
        "--loan-topic",
        default="berka_loans",
        help="Kafka topic name for loan events. Default: berka_loans",
    )
    parser.add_argument(
        "--silver-db",
        default="silver",
        help="Database/schema for the silver loan fact table. Default: silver",
    )
    parser.add_argument(
        "--silver-table",
        default="fact_loan_silver",
        help="Silver loan fact table name. Default: fact_loan_silver",
    )
    parser.add_argument(
        "--gold-db",
        default="gold",
        help="Database/schema for the gold loan fact table. Default: gold",
    )
    parser.add_argument(
        "--gold-table",
        default="fact_loan",
        help="Gold loan fact table name (Iceberg). Default: fact_loan",
    )
    parser.add_argument(
        "--checkpoint-location",
        default="/tmp/berka_fact_loan_checkpoint",
        help="Checkpoint location for the streaming query. Default: /tmp/berka_fact_loan_checkpoint",
    )
    parser.add_argument(
        "--trigger-seconds",
        type=int,
        default=30,
        help="Structured Streaming trigger interval in seconds. Default: 30",
    )
    return parser.parse_args()


def init_tables(spark: SparkSession, args: argparse.Namespace) -> None:
    silver_table_full = f"{args.silver_db}.{args.silver_table}"
    gold_table_full = f"{args.gold_db}.{args.gold_table}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.silver_db}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.gold_db}")

    create_silver_sql = f"""
    CREATE TABLE IF NOT EXISTS {silver_table_full} (
      loan_id BIGINT,
      account_id INT,
      client_id INT,
      disp_id INT,
      district_id INT,
      loan_date DATE,
      amount DOUBLE,
      duration INT,
      payments DOUBLE,
      status STRING,
      ingest_ts TIMESTAMP,
      batch_id BIGINT
    )
    USING parquet
    TBLPROPERTIES ('parquet.compression' = 'snappy')
    """
    spark.sql(create_silver_sql)

    dim_account_table = f"{args.gold_db}.dim_account"

    create_gold_sql = f"""
    CREATE TABLE IF NOT EXISTS {gold_table_full} (
      loan_id BIGINT,
      account_id INT,
      client_id INT,
      disp_id INT,
      district_id INT,
      loan_date DATE,
      amount DOUBLE,
      duration INT,
      payments DOUBLE,
      status STRING,
      account_dim_district_id INT,
      ingest_ts TIMESTAMP,
      batch_id BIGINT
    )
    USING iceberg
    TBLPROPERTIES (
      'write.format.default' = 'parquet',
      'write.parquet.compression-codec' = 'snappy'
    )
    """
    spark.sql(create_gold_sql)

    spark.catalog.refreshTable(silver_table_full)
    spark.catalog.refreshTable(dim_account_table)


def process_batch(spark: SparkSession, batch_df, batch_id: int, args: argparse.Namespace) -> None:
    silver_table_full = f"{args.silver_db}.{args.silver_table}"
    gold_table_full = f"{args.gold_db}.{args.gold_table}"
    dim_account_table = f"{args.gold_db}.dim_account"

    batch_df.createOrReplaceGlobalTempView("loan_kafka_batch")

    loan_schema = """
      loan_id BIGINT,
      account_id INT,
      district_id INT,
      client_id INT,
      disp_id INT,
      date STRING,
      amount DOUBLE,
      duration INT,
      payments DOUBLE,
      status STRING,
      ingest_ts STRING
    """

    insert_silver_sql = f"""
    INSERT INTO {silver_table_full}
    (
      loan_id,
      account_id,
      client_id,
      disp_id,
      district_id,
      loan_date,
      amount,
      duration,
      payments,
      status,
      ingest_ts,
      batch_id
    )
    SELECT
      data.loan_id,
      data.account_id,
      data.client_id,
      data.disp_id,
      data.district_id,
      TO_DATE(data.date)                           AS loan_date,
      data.amount,
      data.duration,
      data.payments,
      data.status,
      TO_TIMESTAMP(data.ingest_ts)                 AS ingest_ts,
      {batch_id}                                   AS batch_id
    FROM (
      SELECT
        from_json(CAST(value AS STRING), '{loan_schema}') AS data
      FROM global_temp.loan_kafka_batch
    ) src
    WHERE data.loan_id IS NOT NULL
      AND data.amount > 0
    """
    spark.sql(insert_silver_sql)

    insert_gold_sql = f"""
    INSERT INTO {gold_table_full}
    (
      loan_id,
      account_id,
      client_id,
      disp_id,
      district_id,
      loan_date,
      amount,
      duration,
      payments,
      status,
      account_dim_district_id,
      ingest_ts,
      batch_id
    )
    SELECT
      s.loan_id,
      s.account_id,
      s.client_id,
      s.disp_id,
      s.district_id,
      s.loan_date,
      s.amount,
      s.duration,
      s.payments,
      s.status,
      da.district_id AS account_dim_district_id,
      s.ingest_ts,
      s.batch_id
    FROM {silver_table_full} s
    LEFT JOIN {dim_account_table} da
      ON s.account_id = da.account_id
     AND da.is_current = TRUE
    WHERE s.batch_id = {batch_id}
    """
    spark.sql(insert_gold_sql)


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("berka_fact_loan_streaming")
        .config("spark.executor.instances", "1")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.security.credentials.hiveserver2.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )

    init_tables(spark, args)

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="manishm" password="Cloudera@123";',
        )
        .option("subscribe", args.loan_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    query = (
        stream_df.writeStream.foreachBatch(
            lambda df, batch_id: process_batch(spark, df, batch_id, args)
        )
        .option("checkpointLocation", args.checkpoint_location)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
