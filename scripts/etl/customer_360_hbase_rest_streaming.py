#!/usr/bin/env python3
"""
Customer 360 streaming job using the HBase REST API.

This variant:
  - Builds a Customer 360 view from Kafka fact streams + gold dimensions
    (dim_client, dim_account) using Spark SQL.
  - Writes each micro-batch into COD HBase `customer360` via the HBase REST
    endpoint instead of using the native HBase client or the Spark3 connector.

Notes and assumptions:
  - This job makes HTTP calls from the Spark driver in each micro-batch. It is
    intended for demo / moderate-volume use, not ultra-high-throughput.
  - HBase REST is fronted by Knox. Basic auth with the workload user is used
    (see --hbase-rest-user / --hbase-rest-password).
"""

import argparse
import base64
import json
from typing import Any, Dict

import requests
from pyspark.sql import DataFrame, SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Customer 360 streaming job (Kafka → HBase via REST)."
    )

    # Kafka + schema configuration
    parser.add_argument(
        "--bootstrap-servers",
        default=(
            "kafka-demo-corebroker0.maybank1.xfaz-gdb4.cloudera.site:9093,"
            "kafka-demo-corebroker1.maybank1.xfaz-gdb4.cloudera.site:9093,"
            "kafka-demo-corebroker2.maybank1.xfaz-gdb4.cloudera.site:9093"
        ),
        help="Kafka bootstrap servers (host:port, comma separated).",
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

    # Streaming + checkpoint configuration
    parser.add_argument(
        "--checkpoint-location",
        default="/tmp/berka_customer360_hbase_rest_checkpoint",
        help="Checkpoint base location for the streaming queries.",
    )
    parser.add_argument(
        "--trigger-seconds",
        type=int,
        default=30,
        help="Structured Streaming trigger interval in seconds. Default: 30",
    )
    parser.add_argument(
        "--kafka-group-id",
        default="berka_customer360_hbase_rest_cg_v1",
        help="Kafka consumer group id for this streaming job.",
    )

    # HBase REST configuration
    parser.add_argument(
        "--hbase-rest-url",
        default=(
            "https://cod-29b4gj7gde0p-gateway0.maybank1.xfaz-gdb4.cloudera.site"
            "/cod-29b4gj7gde0p/cdp-proxy-api/hbase"
        ),
        help="Base URL for the COD HBase REST service (no trailing slash).",
    )
    parser.add_argument(
        "--hbase-namespace",
        default="default",
        help="HBase namespace for the customer360 table. Default: default",
    )
    parser.add_argument(
        "--hbase-table",
        default="customer360",
        help="HBase table name (without namespace). Default: customer360",
    )
    parser.add_argument(
        "--hbase-column-family",
        default="f",
        help="HBase column family for Customer 360 attributes. Default: f",
    )
    parser.add_argument(
        "--hbase-rest-user",
        default="manishm",
        help="Username for HBase REST basic auth.",
    )
    parser.add_argument(
        "--hbase-rest-password",
        default="Cloudera@123",
        help="Password for HBase REST basic auth.",
    )
    parser.add_argument(
        "--max-rows-per-batch",
        type=int,
        default=1000,
        help=(
            "Maximum number of Customer360 rows to send to HBase per micro-batch. "
            "Additional rows in the batch are skipped to keep the demo safe."
        ),
    )

    return parser.parse_args()


def init_dimension_views(spark: SparkSession, args: argparse.Namespace) -> None:
    dim_client_table = f"{args.gold_db}.dim_client"
    dim_account_table = f"{args.gold_db}.dim_account"

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


def _b64(s: bytes) -> str:
    return base64.b64encode(s).decode("ascii")


def _build_hbase_row_payload(
    row: Any, cf: str, rowkey: str
) -> Dict[str, Any]:
    """
    Build the HBase REST JSON payload for a single Customer 360 row.
    """
    cells = []

    def add_cell(col_name: str, value: Any) -> None:
        if value is None:
            return
        col = f"{cf}:{col_name}"
        cells.append(
            {
                "column": _b64(col.encode("utf-8")),
                "$": _b64(str(value).encode("utf-8")),
            }
        )

    add_cell("client_id", row.client_id)
    add_cell("birth_number", row.birth_number)
    add_cell("client_district_id", row.client_district_id)
    add_cell("account_id", row.account_id)
    add_cell("account_district_id", row.account_district_id)
    add_cell("loan_id", row.loan_id)
    add_cell("last_loan_amount", row.last_loan_amount)
    add_cell("last_loan_status", row.last_loan_status)
    add_cell("last_loan_ts", row.last_loan_ts)
    add_cell("last_order_amount", row.last_order_amount)
    add_cell("last_order_k_symbol", row.last_order_k_symbol)
    add_cell("last_order_ts", row.last_order_ts)
    add_cell("last_trans_amount", row.last_trans_amount)
    add_cell("last_trans_type", row.last_trans_type)
    add_cell("last_trans_operation", row.last_trans_operation)
    add_cell("last_trans_ts", row.last_trans_ts)

    return {
        "Row": [
            {
                "key": _b64(rowkey.encode("utf-8")),
                "Cell": cells,
            }
        ]
    }


def write_customer360_to_hbase_rest(
    df: DataFrame, args: argparse.Namespace
) -> None:
    """
    Write a Customer 360 batch DataFrame into HBase using the REST API.

    For each row:
      - Row key: client_id as string.
      - Columns: all other attributes in the configured column family.
    """
    if df.rdd.isEmpty():
        return

    # Collect a bounded number of rows on the driver for the demo.
    rows = df.limit(args.max_rows_per_batch).collect()
    if not rows:
        return

    base_url = args.hbase_rest_url.rstrip("/")
    table_name = f"{args.hbase_namespace}:{args.hbase_table}"
    cf = args.hbase_column_family

    session = requests.Session()
    session.auth = (args.hbase_rest_user, args.hbase_rest_password)
    headers = {"Content-Type": "application/json"}

    for row in rows:
        rowkey = str(row.client_id)
        payload = _build_hbase_row_payload(row, cf, rowkey)
        url = f"{base_url}/{table_name}/{rowkey}"
        try:
            resp = session.put(url, headers=headers, data=json.dumps(payload), timeout=30)
            if resp.status_code not in (200, 201):
                print(
                    f"WARN: HBase REST write failed for row {rowkey}: "
                    f"{resp.status_code} {resp.text[:200]}"
                )
        except Exception as exc:  # noqa: BLE001
            print(f"WARN: Exception during HBase REST write for row {rowkey}: {exc}")


def process_loan_batch(
    spark: SparkSession, batch_df: DataFrame, batch_id: int, args: argparse.Namespace
) -> None:
    if batch_df.isEmpty():
        return

    batch_df.createOrReplaceGlobalTempView("loan_kafka_batch")

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
        FROM global_temp.loan_kafka_batch
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
    WHERE latest.rn = 1
    """

    customer_df = spark.sql(customer_loan_sql)
    write_customer360_to_hbase_rest(customer_df, args)


def process_order_batch(
    spark: SparkSession, batch_df: DataFrame, batch_id: int, args: argparse.Namespace
) -> None:
    if batch_df.isEmpty():
        return

    batch_df.createOrReplaceGlobalTempView("order_kafka_batch")

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
        FROM global_temp.order_kafka_batch
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
    write_customer360_to_hbase_rest(customer_df, args)


def process_trans_batch(
    spark: SparkSession, batch_df: DataFrame, batch_id: int, args: argparse.Namespace
) -> None:
    if batch_df.isEmpty():
        return

    batch_df.createOrReplaceGlobalTempView("trans_kafka_batch")

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
        data.date,
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
        FROM global_temp.trans_kafka_batch
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
    write_customer360_to_hbase_rest(customer_df, args)


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("berka_customer_360_hbase_rest")
        .config("spark.executor.instances", "1")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.security.credentials.hiveserver2.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )

    init_dimension_views(spark, args)

    loan_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("kafka.group.id", args.kafka_group_id)
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

    order_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("kafka.group.id", args.kafka_group_id)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="manishm" password="Cloudera@123";',
        )
        .option("subscribe", args.order_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    trans_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("kafka.group.id", args.kafka_group_id)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="manishm" password="Cloudera@123";',
        )
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

