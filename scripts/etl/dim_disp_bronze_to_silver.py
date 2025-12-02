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
        .config("spark.security.credentials.hiveserver2.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )

    bronze_table_full = f"{args.bronze_db}.{args.bronze_table}"
    silver_table_full = f"{args.silver_db}.{args.silver_table}"
    dq_table_full = f"{args.silver_db}.dq_disp"

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {dq_table_full} (
          disp_id    STRING,
          client_id  STRING,
          account_id STRING,
          type       STRING,
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
      disp_id,
      client_id,
      account_id,
      type,
      dq_date,
      dq_reason
    )
    SELECT
      disp_id,
      client_id,
      account_id,
      type,
      current_date AS dq_date,
      'Invalid disposition identifiers or type' AS dq_reason
    FROM {bronze_table_full}
    WHERE NOT (
      disp_id   RLIKE '^[0-9]+$' AND
      client_id RLIKE '^[0-9]+$' AND
      account_id RLIKE '^[0-9]+$' AND
      type IN ('OWNER', 'DISPONENT')
    )
    """
    spark.sql(dq_insert_sql)

    create_silver_sql = f"""
    CREATE TABLE IF NOT EXISTS {silver_table_full} (
      disp_id    INT,
      client_id  INT,
      account_id INT,
      disp_type  STRING
    )
    USING parquet
    TBLPROPERTIES ('parquet.compression' = 'snappy')
    """
    spark.sql(create_silver_sql)

    insert_silver_sql = f"""
    INSERT OVERWRITE TABLE {silver_table_full}
    SELECT
      CAST(disp_id AS INT)    AS disp_id,
      CAST(client_id AS INT)  AS client_id,
      CAST(account_id AS INT) AS account_id,
      type                    AS disp_type
    FROM {bronze_table_full}
    WHERE disp_id   RLIKE '^[0-9]+$'
      AND client_id RLIKE '^[0-9]+$'
      AND account_id RLIKE '^[0-9]+$'
      AND type IN ('OWNER', 'DISPONENT')
    """

    spark.sql(insert_silver_sql)

    spark.stop()


if __name__ == "__main__":
    main()
