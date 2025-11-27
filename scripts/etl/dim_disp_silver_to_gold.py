#!/usr/bin/env python3
"""
Silver → Gold SCD Type 2 job for the `disp` (disposition) dimension using Iceberg tables.
"""

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Silver → Gold Spark SQL job for the disp dimension (SCD Type 2, Iceberg)."
    )
    parser.add_argument(
        "--silver-db",
        default="silver",
        help="Database/schema that holds the silver disp table. Default: silver",
    )
    parser.add_argument(
        "--gold-db",
        default="gold",
        help="Database/schema for the gold dim_disp table. Default: gold",
    )
    parser.add_argument(
        "--silver-table",
        default="disp_silver",
        help="Silver table name for disp data. Default: disp_silver",
    )
    parser.add_argument(
        "--gold-table",
        default="dim_disp",
        help="Gold dimension table name (Iceberg). Default: dim_disp",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("dim_disp_silver_to_gold_scd2_sql")
        .enableHiveSupport()
        .getOrCreate()
    )

    silver_table_full = f"{args.silver_db}.{args.silver_table}"
    gold_table_full = f"{args.gold_db}.{args.gold_table}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.gold_db}")

    create_gold_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {gold_table_full} (
      disp_id INT,
      client_id INT,
      account_id INT,
      disp_type STRING,
      effective_from TIMESTAMP,
      effective_to TIMESTAMP,
      is_current BOOLEAN,
      scd_version INT
    )
    USING iceberg
    TBLPROPERTIES (
      'write.format.default' = 'parquet',
      'write.parquet.compression-codec' = 'snappy'
    )
    """
    spark.sql(create_gold_table_sql)

    insert_new_sql = f"""
    INSERT INTO {gold_table_full}
    (
      disp_id,
      client_id,
      account_id,
      disp_type,
      effective_from,
      effective_to,
      is_current,
      scd_version
    )
    SELECT
      s.disp_id,
      s.client_id,
      s.account_id,
      s.disp_type,
      current_timestamp()              AS effective_from,
      TIMESTAMP '9999-12-31 23:59:59' AS effective_to,
      TRUE                            AS is_current,
      1                               AS scd_version
    FROM {silver_table_full} s
    LEFT JOIN {gold_table_full} g
      ON s.disp_id = g.disp_id
     AND g.is_current = TRUE
    WHERE g.disp_id IS NULL
    """
    spark.sql(insert_new_sql)

    insert_changed_sql = f"""
    INSERT INTO {gold_table_full}
    (
      disp_id,
      client_id,
      account_id,
      disp_type,
      effective_from,
      effective_to,
      is_current,
      scd_version
    )
    SELECT
      s.disp_id,
      s.client_id,
      s.account_id,
      s.disp_type,
      current_timestamp()              AS effective_from,
      TIMESTAMP '9999-12-31 23:59:59' AS effective_to,
      TRUE                            AS is_current,
      g.scd_version + 1               AS scd_version
    FROM {silver_table_full} s
    JOIN {gold_table_full} g
      ON s.disp_id = g.disp_id
     AND g.is_current = TRUE
    WHERE NOT (
      s.client_id  <=> g.client_id  AND
      s.account_id <=> g.account_id AND
      s.disp_type  <=> g.disp_type
    )
    """
    spark.sql(insert_changed_sql)

    expire_old_sql = f"""
    UPDATE {gold_table_full} AS g
    SET
      g.effective_to = current_timestamp(),
      g.is_current   = FALSE
    WHERE g.is_current = TRUE
      AND g.disp_id IN (
        SELECT g2.disp_id
        FROM {silver_table_full} s
        JOIN {gold_table_full} g2
          ON s.disp_id = g2.disp_id
         AND g2.is_current = TRUE
        WHERE NOT (
          s.client_id  <=> g2.client_id  AND
          s.account_id <=> g2.account_id AND
          s.disp_type  <=> g2.disp_type
        )
      )
    """
    spark.sql(expire_old_sql)

    spark.stop()


if __name__ == "__main__":
    main()
