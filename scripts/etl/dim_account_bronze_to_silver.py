#!/usr/bin/env python3
"""
Bronze → Silver job for the `account` dimension.

Reads raw account data (populated by NiFi) from the bronze layer and writes
cleaned, typed records into the silver layer using Spark SQL.
"""

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bronze → Silver Spark SQL job for the account dimension."
    )
    parser.add_argument(
        "--bronze-db",
        default="bronze",
        help="Database/schema that holds the bronze account table. Default: bronze",
    )
    parser.add_argument(
        "--silver-db",
        default="silver",
        help="Database/schema for the silver account table. Default: silver",
    )
    parser.add_argument(
        "--bronze-table",
        default="account_bronze",
        help="Bronze table name for account data. Default: account_bronze",
    )
    parser.add_argument(
        "--silver-table",
        default="account_silver",
        help="Silver table name for account data. Default: account_silver",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("dim_account_bronze_to_silver_sql")
        .enableHiveSupport()
        .getOrCreate()
    )

    bronze_table_full = f"{args.bronze_db}.{args.bronze_table}"
    silver_table_full = f"{args.silver_db}.{args.silver_table}"
    dq_table_full = f"{args.silver_db}.dq_account"

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {dq_table_full} (
          account_id STRING,
          district_id STRING,
          frequency  STRING,
          date       STRING,
          dq_date    DATE,
          dq_reason  STRING
        )
        USING parquet
        TBLPROPERTIES ('parquet.compression' = 'snappy')
        """
    )

    dq_insert_sql = f"""
    INSERT INTO {dq_table_full}
    (
      account_id,
      district_id,
      frequency,
      date,
      dq_date,
      dq_reason
    )
    SELECT
      account_id,
      district_id,
      frequency,
      date,
      current_date AS dq_date,
      'Invalid account/district identifiers, frequency, or date format' AS dq_reason
    FROM {bronze_table_full}
    WHERE NOT (
      account_id  RLIKE '^[0-9]+$' AND
      district_id RLIKE '^[0-9]+$' AND
      frequency   IS NOT NULL AND frequency <> '' AND
      date        RLIKE '^[0-9]{6}$'
    )
    """
    spark.sql(dq_insert_sql)

    create_silver_sql = f"""
    CREATE OR REPLACE TABLE {silver_table_full}
    USING parquet
    TBLPROPERTIES ('parquet.compression' = 'snappy') AS
    SELECT
      CAST(account_id AS INT)   AS account_id,
      CAST(district_id AS INT)  AS district_id,
      frequency                 AS frequency,
      TO_DATE(date, 'yyMMdd')   AS open_date
    FROM {bronze_table_full}
    WHERE account_id  RLIKE '^[0-9]+$'
      AND district_id RLIKE '^[0-9]+$'
      AND frequency   IS NOT NULL
      AND frequency   <> ''
      AND date        RLIKE '^[0-9]{6}$'
    """

    spark.sql(create_silver_sql)

    spark.stop()


if __name__ == "__main__":
    main()
