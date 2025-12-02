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
        .config("spark.security.credentials.hiveserver2.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )

    bronze_table_full = f"{args.bronze_db}.{args.bronze_table}"
    silver_table_full = f"{args.silver_db}.{args.silver_table}"
    dq_table_full = f"{args.silver_db}.dq_district"

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {dq_table_full} (
          A1       STRING,
          A2       STRING,
          A3       STRING,
          A4       STRING,
          A5       STRING,
          A6       STRING,
          A7       STRING,
          A8       STRING,
          A9       STRING,
          A10      STRING,
          A11      STRING,
          A12      STRING,
          A13      STRING,
          A14      STRING,
          A15      STRING,
          A16      STRING,
          dq_date  DATE,
          dq_reason STRING
        )
        USING parquet
        TBLPROPERTIES ('parquet.compression' = 'snappy')
        """
    )

    dq_insert_sql = f"""
    INSERT INTO {dq_table_full}
    (
      A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16,
      dq_date,
      dq_reason
    )
    SELECT
      A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16,
      current_date AS dq_date,
      'Invalid district id or missing name/region' AS dq_reason
    FROM {bronze_table_full}
    WHERE NOT (
      A1 RLIKE '^[0-9]+$' AND
      A2 IS NOT NULL AND A2 <> '' AND
      A3 IS NOT NULL AND A3 <> ''
    )
    """
    spark.sql(dq_insert_sql)

    create_silver_sql = f"""
    CREATE TABLE IF NOT EXISTS {silver_table_full} (
      district_id   INT,
      district_name STRING,
      region        STRING,
      A4            INT,
      A5            INT,
      A6            INT,
      A7            INT,
      A8            INT,
      A9            INT,
      A10           DOUBLE,
      A11           INT,
      A12           DOUBLE,
      A13           DOUBLE,
      A14           INT,
      A15           INT,
      A16           INT
    )
    USING parquet
    TBLPROPERTIES ('parquet.compression' = 'snappy')
    """
    spark.sql(create_silver_sql)

    insert_silver_sql = f"""
    INSERT OVERWRITE TABLE {silver_table_full}
    SELECT
      CAST(A1 AS INT)     AS district_id,
      A2                  AS district_name,
      A3                  AS region,
      CAST(A4 AS INT)     AS A4,
      CAST(A5 AS INT)     AS A5,
      CAST(A6 AS INT)     AS A6,
      CAST(A7 AS INT)     AS A7,
      CAST(A8 AS INT)     AS A8,
      CAST(A9 AS INT)     AS A9,
      CAST(A10 AS DOUBLE) AS A10,
      CAST(A11 AS INT)    AS A11,
      CAST(A12 AS DOUBLE) AS A12,
      CAST(A13 AS DOUBLE) AS A13,
      CAST(A14 AS INT)    AS A14,
      CAST(A15 AS INT)    AS A15,
      CAST(A16 AS INT)    AS A16
    FROM {bronze_table_full}
    WHERE A1 RLIKE '^[0-9]+$'
      AND A2 IS NOT NULL AND A2 <> ''
      AND A3 IS NOT NULL AND A3 <> ''
    """

    spark.sql(insert_silver_sql)

    spark.stop()


if __name__ == "__main__":
    main()
