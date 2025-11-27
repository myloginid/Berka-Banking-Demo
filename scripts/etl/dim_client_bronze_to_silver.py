#!/usr/bin/env python3
"""
Bronze → Silver job for the `client` dimension.

Reads raw client data (populated by NiFi) from the bronze layer and writes
cleaned, typed records into the silver layer using Spark SQL.

Typical usage (example):
  spark-submit scripts/etl/dim_client_bronze_to_silver.py \\
    --bronze-db bronze \\
    --silver-db silver \\
    --bronze-table client_bronze \\
    --silver-table client_silver
"""

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bronze → Silver Spark SQL job for the client dimension."
    )
    parser.add_argument(
        "--bronze-db",
        default="bronze",
        help="Database/schema that holds the bronze client table. Default: bronze",
    )
    parser.add_argument(
        "--silver-db",
        default="silver",
        help="Database/schema for the silver client table. Default: silver",
    )
    parser.add_argument(
        "--bronze-table",
        default="client_bronze",
        help="Bronze table name for client data. Default: client_bronze",
    )
    parser.add_argument(
        "--silver-table",
        default="client_silver",
        help="Silver table name for client data. Default: client_silver",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("dim_client_bronze_to_silver_sql")
        .enableHiveSupport()
        .getOrCreate()
    )

    bronze_table_full = f"{args.bronze_db}.{args.bronze_table}"
    silver_table_full = f"{args.silver_db}.{args.silver_table}"

    create_silver_sql = f"""
    CREATE OR REPLACE TABLE {silver_table_full}
    USING parquet
    TBLPROPERTIES ('parquet.compression' = 'snappy') AS
    SELECT
      CAST(client_id AS INT)   AS client_id,
      birth_number             AS birth_number,
      CAST(district_id AS INT) AS district_id
    FROM {bronze_table_full}
    """

    spark.sql(create_silver_sql)

    spark.stop()


if __name__ == "__main__":
    main()
