#!/usr/bin/env python3
"""
Bronze → Silver job for the `district` dimension.

Reads raw district data (populated by NiFi) from the bronze layer and writes
cleaned, typed records into the silver layer.

Typical usage (example):
  spark-submit dim_district_bronze_to_silver.py \\
    --bronze-db bronze \\
    --silver-db silver \\
    --bronze-table district_bronze \\
    --silver-table district_silver
"""

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bronze → Silver Spark job for the district dimension."
    )
    parser.add_argument(
        "--bronze-db",
        default="bronze",
        help="Database/schema that holds the bronze district table. Default: bronze",
    )
    parser.add_argument(
        "--silver-db",
        default="silver",
        help="Database/schema for the silver district table. Default: silver",
    )
    parser.add_argument(
        "--bronze-table",
        default="district_bronze",
        help="Bronze table name for district data. Default: district_bronze",
    )
    parser.add_argument(
        "--silver-table",
        default="district_silver",
        help="Silver table name for district data. Default: district_silver",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("dim_district_bronze_to_silver")
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
      CAST(A1 AS INT)    AS district_id,
      A2                 AS district_name,
      A3                 AS region,
      CAST(A4 AS INT)    AS A4,
      CAST(A5 AS INT)    AS A5,
      CAST(A6 AS INT)    AS A6,
      CAST(A7 AS INT)    AS A7,
      CAST(A8 AS INT)    AS A8,
      CAST(A9 AS INT)    AS A9,
      CAST(A10 AS DOUBLE) AS A10,
      CAST(A11 AS INT)   AS A11,
      CAST(A12 AS DOUBLE) AS A12,
      CAST(A13 AS DOUBLE) AS A13,
      CAST(A14 AS INT)   AS A14,
      CAST(A15 AS INT)   AS A15,
      CAST(A16 AS INT)   AS A16
    FROM {bronze_table_full}
    """

    spark.sql(create_silver_sql)

    spark.stop()


if __name__ == "__main__":
    main()
