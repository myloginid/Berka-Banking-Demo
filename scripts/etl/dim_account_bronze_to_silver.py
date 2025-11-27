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
    """

    spark.sql(create_silver_sql)

    spark.stop()


if __name__ == "__main__":
    main()
