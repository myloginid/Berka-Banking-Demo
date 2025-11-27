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
    dq_table_full = f"{args.silver_db}.dq_client"

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {dq_table_full} (
          client_id    STRING,
          birth_number STRING,
          district_id  STRING,
          dq_date      DATE,
          dq_reason    STRING
        )
        USING parquet
        TBLPROPERTIES ('parquet.compression' = 'snappy')
        """
    )

    dq_insert_sql = f"""
    INSERT INTO {dq_table_full}
    (
      client_id,
      birth_number,
      district_id,
      dq_date,
      dq_reason
    )
    SELECT
      client_id,
      birth_number,
      district_id,
      current_date    AS dq_date,
      'Invalid client_id/district_id format or missing birth_number' AS dq_reason
    FROM {bronze_table_full}
    WHERE NOT (
      client_id  RLIKE '^[0-9]+$' AND
      district_id RLIKE '^[0-9]+$' AND
      birth_number IS NOT NULL AND birth_number <> ''
    )
    """
    spark.sql(dq_insert_sql)

    create_silver_sql = f"""
    CREATE OR REPLACE TABLE {silver_table_full}
    USING parquet
    TBLPROPERTIES ('parquet.compression' = 'snappy') AS
    SELECT
      CAST(client_id AS INT)   AS client_id,
      birth_number             AS birth_number,
      CAST(district_id AS INT) AS district_id
    FROM {bronze_table_full}
    WHERE client_id  RLIKE '^[0-9]+$'
      AND district_id RLIKE '^[0-9]+$'
      AND birth_number IS NOT NULL
      AND birth_number <> ''
    """

    spark.sql(create_silver_sql)

    spark.stop()


if __name__ == "__main__":
    main()
