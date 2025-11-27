#!/usr/bin/env python3
"""
Silver → Gold SCD Type 2 job for the `district` dimension using Iceberg tables.

Reads cleaned district records from the silver layer and maintains a
dimensional SCD2 table in the gold layer backed by Iceberg.

Typical usage (example):
  spark-submit dim_district_silver_to_gold.py \\
    --silver-db silver \\
    --gold-db gold \\
    --silver-table district_silver \\
    --gold-table dim_district
"""

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Silver → Gold Spark job for the district dimension (SCD Type 2, Iceberg)."
    )
    parser.add_argument(
        "--silver-db",
        default="silver",
        help="Database/schema that holds the silver district table. Default: silver",
    )
    parser.add_argument(
        "--gold-db",
        default="gold",
        help="Database/schema for the gold dim_district table. Default: gold",
    )
    parser.add_argument(
        "--silver-table",
        default="district_silver",
        help="Silver table name for district data. Default: district_silver",
    )
    parser.add_argument(
        "--gold-table",
        default="dim_district",
        help="Gold dimension table name (Iceberg). Default: dim_district",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("dim_district_silver_to_gold_scd2")
        .enableHiveSupport()
        .getOrCreate()
    )

    silver_table_full = f"{args.silver_db}.{args.silver_table}"
    gold_table_full = f"{args.gold_db}.{args.gold_table}"

    create_database_sql = f"CREATE DATABASE IF NOT EXISTS {args.gold_db}"
    spark.sql(create_database_sql)

    create_gold_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {gold_table_full} (
      district_id INT,
      district_name STRING,
      region STRING,
      A4 INT,
      A5 INT,
      A6 INT,
      A7 INT,
      A8 INT,
      A9 INT,
      A10 DOUBLE,
      A11 INT,
      A12 DOUBLE,
      A13 DOUBLE,
      A14 INT,
      A15 INT,
      A16 INT,
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
      district_id,
      district_name,
      region,
      A4,
      A5,
      A6,
      A7,
      A8,
      A9,
      A10,
      A11,
      A12,
      A13,
      A14,
      A15,
      A16,
      effective_from,
      effective_to,
      is_current,
      scd_version
    )
    SELECT
      s.district_id,
      s.district_name,
      s.region,
      s.A4,
      s.A5,
      s.A6,
      s.A7,
      s.A8,
      s.A9,
      s.A10,
      s.A11,
      s.A12,
      s.A13,
      s.A14,
      s.A15,
      s.A16,
      current_timestamp() AS effective_from,
      TIMESTAMP '9999-12-31 23:59:59' AS effective_to,
      TRUE AS is_current,
      1 AS scd_version
    FROM {silver_table_full} s
    LEFT JOIN {gold_table_full} g
      ON s.district_id = g.district_id
     AND g.is_current = TRUE
    WHERE g.district_id IS NULL
    """
    spark.sql(insert_new_sql)

    insert_changed_sql = f"""
    INSERT INTO {gold_table_full}
    (
      district_id,
      district_name,
      region,
      A4,
      A5,
      A6,
      A7,
      A8,
      A9,
      A10,
      A11,
      A12,
      A13,
      A14,
      A15,
      A16,
      effective_from,
      effective_to,
      is_current,
      scd_version
    )
    SELECT
      s.district_id,
      s.district_name,
      s.region,
      s.A4,
      s.A5,
      s.A6,
      s.A7,
      s.A8,
      s.A9,
      s.A10,
      s.A11,
      s.A12,
      s.A13,
      s.A14,
      s.A15,
      s.A16,
      current_timestamp() AS effective_from,
      TIMESTAMP '9999-12-31 23:59:59' AS effective_to,
      TRUE AS is_current,
      g.scd_version + 1 AS scd_version
    FROM {silver_table_full} s
    JOIN {gold_table_full} g
      ON s.district_id = g.district_id
     AND g.is_current = TRUE
    WHERE NOT (
      s.district_name <=> g.district_name AND
      s.region        <=> g.region        AND
      s.A4            <=> g.A4            AND
      s.A5            <=> g.A5            AND
      s.A6            <=> g.A6            AND
      s.A7            <=> g.A7            AND
      s.A8            <=> g.A8            AND
      s.A9            <=> g.A9            AND
      s.A10           <=> g.A10           AND
      s.A11           <=> g.A11           AND
      s.A12           <=> g.A12           AND
      s.A13           <=> g.A13           AND
      s.A14           <=> g.A14           AND
      s.A15           <=> g.A15           AND
      s.A16           <=> g.A16
    )
    """
    spark.sql(insert_changed_sql)

    expire_old_sql = f"""
    UPDATE {gold_table_full} AS g
    SET
      g.effective_to = current_timestamp(),
      g.is_current   = FALSE
    WHERE g.is_current = TRUE
      AND g.district_id IN (
        SELECT g2.district_id
        FROM {silver_table_full} s
        JOIN {gold_table_full} g2
          ON s.district_id = g2.district_id
         AND g2.is_current = TRUE
        WHERE NOT (
          s.district_name <=> g2.district_name AND
          s.region        <=> g2.region        AND
          s.A4            <=> g2.A4            AND
          s.A5            <=> g2.A5            AND
          s.A6            <=> g2.A6            AND
          s.A7            <=> g2.A7            AND
          s.A8            <=> g2.A8            AND
          s.A9            <=> g2.A9            AND
          s.A10           <=> g2.A10           AND
          s.A11           <=> g2.A11           AND
          s.A12           <=> g2.A12           AND
          s.A13           <=> g2.A13           AND
          s.A14           <=> g2.A14           AND
          s.A15           <=> g2.A15           AND
          s.A16           <=> g2.A16
        )
      )
    """
    spark.sql(expire_old_sql)

    spark.stop()


if __name__ == "__main__":
    main()
