#!/usr/bin/env python3
"""
Customer 360 streaming job writing to HBase.

This Spark Structured Streaming job:
  - Reads static dimension data from Iceberg dimension tables (dim_client, dim_account).
  - Consumes live loan, order, and transaction events from Kafka.
  - Builds a simple "Customer 360" view per client (latest event context).
  - Writes one row per customer into HBase via a REST endpoint.

All transformations are implemented with Spark SQL; DataFrames are only used
for streaming input/output plumbing and calling the HBase sink.

Example:
  spark-submit scripts/etl/customer_360_hbase_streaming.py \\
    --bootstrap-servers localhost:9092 \\
    --loan-topic berka_loans --order-topic berka_orders --trans-topic berka_trans \\
    --gold-db gold \\
    --checkpoint-location /tmp/berka_customer360_chk \\
    --hbase-rest-url http://hbase-rest-host:8080 \\
    --hbase-table customer360 \\
    --trigger-seconds 30
"""

import argparse
import json
from typing import Iterator

import requests
from pyspark.sql import SparkSession, DataFrame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Customer 360 streaming job (Kafka → HBase, using Iceberg dimensions)."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (host:port, comma separated). Default: localhost:9092",
    )
    parser.add_argument(
        "--loan-topic",
        default="berka_loans",
        help="Kafka topic name for loan events. Default: berka_loans",
    )
    parser.add_argument(
        "--order-topic",
        default="berka_orders",
        help="Kafka topic name for order events. Default: berka_orders",
    )
    parser.add_argument(
        "--trans-topic",
        default="berka_trans",
        help="Kafka topic name for transaction events. Default: berka_trans",
    )
    parser.add_argument(
        "--gold-db",
        default="gold",
        help="Database/schema where Iceberg dimensions live (dim_client, dim_account). Default: gold",
    )
    parser.add_argument(
        "--checkpoint-location",
        default="/tmp/berka_customer360_checkpoint",
        help="Checkpoint location for the streaming queries. Default: /tmp/berka_customer360_checkpoint",
    )
    parser.add_argument(
        "--trigger-seconds",
        type=int,
        default=30,
        help="Structured Streaming trigger interval in seconds. Default: 30",
    )
    parser.add_argument(
        "--hbase-rest-url",
        default="http://localhost:8080",
        help="Base URL for the HBase REST endpoint (e.g. http://host:port). Default: http://localhost:8080",
    )
    parser.add_argument(
        "--hbase-table",
        default="customer360",
        help="Target HBase table name for Customer 360 rows. Default: customer360",
    )
    parser.add_argument(
        "--hbase-timeout-seconds",
        type=int,
        default=5,
        help="Timeout in seconds for HBase REST calls. Default: 5",
    )
    return parser.parse_args()


def init_dimension_views(spark: SparkSession, args: argparse.Namespace) -> None:
    """
    Create temporary views for the current snapshot of client and account dimensions,
    and the latest loan default scores per client.
    """
    dim_client_table = f"{args.gold_db}.dim_client"
    dim_account_table = f"{args.gold_db}.dim_account"
    fact_loan_table = f"{args.gold_db}.fact_loan"
    loan_scores_table = f"{args.gold_db}.loan_default_scores"

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW dim_client_current AS
        SELECT *
        FROM {dim_client_table}
        WHERE is_current = TRUE
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW dim_account_current AS
        SELECT *
        FROM {dim_account_table}
        WHERE is_current = TRUE
        """
    )

    # Latest loan default score per client (if scoring has been run).
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW loan_default_score_per_client AS
        WITH scored AS (
          SELECT
            fl.client_id,
            lds.pred_status,
            lds.score_timestamp,
            ROW_NUMBER() OVER (
              PARTITION BY fl.client_id
              ORDER BY lds.score_timestamp DESC
            ) AS rn
          FROM {loan_scores_table} lds
          JOIN {fact_loan_table} fl
            ON fl.loan_id = lds.loan_id
        )
        SELECT
          client_id,
          pred_status,
          score_timestamp
        FROM scored
        WHERE rn = 1
        """
    )


def write_customer360_to_hbase(df: DataFrame, args: argparse.Namespace) -> None:
    """
    Write a Customer 360 batch DataFrame to HBase via REST.

    Assumes the HBase REST endpoint supports idempotent upserts for a given row key.
    """

    def _write_partition(rows: Iterator[object]) -> None:
        session = requests.Session()
        base_url = args.hbase_rest_url.rstrip("/")
        for row in rows:
            record = row.asDict()
            client_id = record.get("client_id")
            if client_id is None:
                continue
            row_key = str(client_id)
            url = f"{base_url}/{args.hbase_table}/{row_key}"
            payload = json.dumps(record, default=str)
            try:
                session.post(url, data=payload, timeout=args.hbase_timeout_seconds, headers={"Content-Type": "application/json"})
            except Exception:
                # For POC purposes we swallow errors; production should log/handle retries.
                continue

    df.foreachPartition(_write_partition)


def process_loan_batch(spark: SparkSession, batch_df: DataFrame, batch_id: int, args: argparse.Namespace) -> None:
    """
    Process a loan micro-batch and upsert Customer 360 rows into HBase.
    """
    if batch_df.isEmpty():
        return

    batch_df.createOrReplaceTempView("loan_kafka_batch")

    loan_schema = (
        "loan_id BIGINT, account_id INT, district_id INT, client_id INT, disp_id INT, "
        "date STRING, amount DOUBLE, duration INT, payments DOUBLE, status STRING, ingest_ts STRING"
    )

    customer_loan_sql = f"""
    WITH parsed AS (
      SELECT
        data.loan_id,
        data.account_id,
        data.client_id,
        data.district_id,
        data.amount,
        data.status,
        TO_TIMESTAMP(data.ingest_ts) AS ingest_ts
      FROM (
        SELECT
          from_json(CAST(value AS STRING), '{loan_schema}') AS data
        FROM loan_kafka_batch
      ) src
      WHERE data.loan_id IS NOT NULL
    ),
    latest_per_client AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY ingest_ts DESC) AS rn
      FROM parsed
    )
    SELECT
      c.client_id,
      c.birth_number,
      c.district_id              AS client_district_id,
      a.account_id,
      a.district_id              AS account_district_id,
      latest.loan_id,
      latest.amount              AS last_loan_amount,
      latest.status              AS last_loan_status,
      latest.ingest_ts           AS last_loan_ts,
      s.pred_status              AS last_default_pred_status,
      s.score_timestamp          AS last_default_score_ts,
      CAST(NULL AS DOUBLE)       AS last_order_amount,
      CAST(NULL AS STRING)       AS last_order_k_symbol,
      CAST(NULL AS TIMESTAMP)    AS last_order_ts,
      CAST(NULL AS DOUBLE)       AS last_trans_amount,
      CAST(NULL AS STRING)       AS last_trans_type,
      CAST(NULL AS STRING)       AS last_trans_operation,
      CAST(NULL AS TIMESTAMP)    AS last_trans_ts
    FROM latest_per_client latest
    LEFT JOIN dim_client_current c
      ON latest.client_id = c.client_id
    LEFT JOIN dim_account_current a
      ON latest.account_id = a.account_id
    LEFT JOIN loan_default_score_per_client s
      ON latest.client_id = s.client_id
    WHERE latest.rn = 1
    """

    customer_df = spark.sql(customer_loan_sql)
    write_customer360_to_hbase(customer_df, args)


def process_order_batch(spark: SparkSession, batch_df: DataFrame, batch_id: int, args: argparse.Namespace) -> None:
    """
    Process an order micro-batch and upsert Customer 360 rows into HBase.
    """
    if batch_df.isEmpty():
        return

    batch_df.createOrReplaceTempView("order_kafka_batch")

    order_schema = (
        "order_id BIGINT, account_id INT, district_id INT, client_id INT, disp_id INT, "
        "bank_to STRING, account_to STRING, amount DOUBLE, k_symbol STRING, ingest_ts STRING"
    )

    customer_order_sql = f"""
    WITH parsed AS (
      SELECT
        data.order_id,
        data.account_id,
        data.client_id,
        data.district_id,
        data.bank_to,
        data.account_to,
        data.amount,
        data.k_symbol,
        TO_TIMESTAMP(data.ingest_ts) AS ingest_ts
      FROM (
        SELECT
          from_json(CAST(value AS STRING), '{order_schema}') AS data
        FROM order_kafka_batch
      ) src
      WHERE data.order_id IS NOT NULL
    ),
    latest_per_client AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY ingest_ts DESC) AS rn
      FROM parsed
    )
    SELECT
      c.client_id,
      c.birth_number,
      c.district_id              AS client_district_id,
      a.account_id,
      a.district_id              AS account_district_id,
      CAST(NULL AS BIGINT)       AS loan_id,
      CAST(NULL AS DOUBLE)       AS last_loan_amount,
      CAST(NULL AS STRING)       AS last_loan_status,
      CAST(NULL AS TIMESTAMP)    AS last_loan_ts,
      latest.amount              AS last_order_amount,
      latest.k_symbol            AS last_order_k_symbol,
      latest.ingest_ts           AS last_order_ts,
      CAST(NULL AS DOUBLE)       AS last_trans_amount,
      CAST(NULL AS STRING)       AS last_trans_type,
      CAST(NULL AS STRING)       AS last_trans_operation,
      CAST(NULL AS TIMESTAMP)    AS last_trans_ts
    FROM latest_per_client latest
    LEFT JOIN dim_client_current c
      ON latest.client_id = c.client_id
    LEFT JOIN dim_account_current a
      ON latest.account_id = a.account_id
    WHERE latest.rn = 1
    """

    customer_df = spark.sql(customer_order_sql)
    write_customer360_to_hbase(customer_df, args)


def process_trans_batch(spark: SparkSession, batch_df: DataFrame, batch_id: int, args: argparse.Namespace) -> None:
    """
    Process a transaction micro-batch and upsert Customer 360 rows into HBase.
    """
    if batch_df.isEmpty():
        return

    batch_df.createOrReplaceTempView("trans_kafka_batch")

    trans_schema = (
        "trans_id BIGINT, account_id INT, district_id INT, client_id INT, disp_id INT, "
        "date STRING, type STRING, operation STRING, amount DOUBLE, balance DOUBLE, "
        "k_symbol STRING, bank STRING, account STRING, ingest_ts STRING"
    )

    customer_trans_sql = f"""
    WITH parsed AS (
      SELECT
        data.trans_id,
        data.account_id,
        data.client_id,
        data.district_id,
        data.type,
        data.operation,
        data.amount,
        data.balance,
        data.k_symbol,
        data.bank,
        data.account,
        TO_TIMESTAMP(data.ingest_ts) AS ingest_ts
      FROM (
        SELECT
          from_json(CAST(value AS STRING), '{trans_schema}') AS data
        FROM trans_kafka_batch
      ) src
      WHERE data.trans_id IS NOT NULL
    ),
    latest_per_client AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY ingest_ts DESC) AS rn
      FROM parsed
    )
    SELECT
      c.client_id,
      c.birth_number,
      c.district_id              AS client_district_id,
      a.account_id,
      a.district_id              AS account_district_id,
      CAST(NULL AS BIGINT)       AS loan_id,
      CAST(NULL AS DOUBLE)       AS last_loan_amount,
      CAST(NULL AS STRING)       AS last_loan_status,
      CAST(NULL AS TIMESTAMP)    AS last_loan_ts,
      CAST(NULL AS DOUBLE)       AS last_order_amount,
      CAST(NULL AS STRING)       AS last_order_k_symbol,
      CAST(NULL AS TIMESTAMP)    AS last_order_ts,
      latest.amount              AS last_trans_amount,
      latest.type                AS last_trans_type,
      latest.operation           AS last_trans_operation,
      latest.ingest_ts           AS last_trans_ts
    FROM latest_per_client latest
    LEFT JOIN dim_client_current c
      ON latest.client_id = c.client_id
    LEFT JOIN dim_account_current a
      ON latest.account_id = a.account_id
    WHERE latest.rn = 1
    """

    customer_df = spark.sql(customer_trans_sql)
    write_customer360_to_hbase(customer_df, args)


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("berka_customer_360_hbase_streaming")
        .enableHiveSupport()
        .getOrCreate()
    )

    init_dimension_views(spark, args)

    loan_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.loan_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    order_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.order_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    trans_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.trans_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    loan_query = (
        loan_stream_df.writeStream.foreachBatch(
            lambda df, batch_id: process_loan_batch(spark, df, batch_id, args)
        )
        .option("checkpointLocation", f"{args.checkpoint_location}/loan")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    order_query = (
        order_stream_df.writeStream.foreachBatch(
            lambda df, batch_id: process_order_batch(spark, df, batch_id, args)
        )
        .option("checkpointLocation", f"{args.checkpoint_location}/order")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    trans_query = (
        trans_stream_df.writeStream.foreachBatch(
            lambda df, batch_id: process_trans_batch(spark, df, batch_id, args)
        )
        .option("checkpointLocation", f"{args.checkpoint_location}/trans")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    loan_query.awaitTermination()
    order_query.awaitTermination()
    trans_query.awaitTermination()


if __name__ == "__main__":
    main()
