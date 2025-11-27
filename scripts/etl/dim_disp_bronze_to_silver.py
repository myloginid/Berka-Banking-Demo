#!/usr/bin/env python3
"""
Bronze → Silver job for the `disp` (disposition) dimension.
"""

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bronze → Silver Spark SQL job for the disp dimension."
    )
    parser.add_argument(
        "--bronze-db",
        default="bronze",
        help="Database/schema that holds the bronze disp table. Default: bronze",
    )
    parser.add_argument(
        "--silver-db",
        default="silver",
        help="Database/schema for the silver disp table. Default: silver",
    )
    parser.add_argument(
        "--bronze-table",
        default="disp_bronze",
        help="Bronze table name for disp data. Default: disp_bronze",
    )
    parser.add_argument(
        "--silver-table",
        default="disp_silver",
        help="Silver table name for disp data. Default: disp_silver",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("dim_disp_bronze_to_silver_sql")
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
      CAST(disp_id AS INT)    AS disp_id,
      CAST(client_id AS INT)  AS client_id,
      CAST(account_id AS INT) AS account_id,
      type                    AS disp_type
    FROM {bronze_table_full}
    """

    spark.sql(create_silver_sql)

    spark.stop()


if __name__ == "__main__":
    main()
