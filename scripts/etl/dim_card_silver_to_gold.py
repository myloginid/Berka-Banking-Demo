#!/usr/bin/env python3
"""
Silver → Gold SCD Type 2 job for the `card` dimension using Iceberg tables.
"""

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Silver → Gold Spark SQL job for the card dimension (SCD Type 2, Iceberg)."
    )
    parser.add_argument(
        "--silver-db",
        default="silver",
        help="Database/schema that holds the silver card table. Default: silver",
    )
    parser.add_argument(
        "--gold-db",
        default="gold",
        help="Database/schema for the gold dim_card table. Default: gold",
    )
    parser.add_argument(
        "--silver-table",
        default="card_silver",
        help="Silver table name for card data. Default: card_silver",
    )
    parser.add_argument(
        "--gold-table",
        default="dim_card",
        help="Gold dimension table name (Iceberg). Default: dim_card",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("dim_card_silver_to_gold_scd2_sql")
        .config("spark.security.credentials.hiveserver2.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )

    silver_table_full = f"{args.silver_db}.{args.silver_table}"
    gold_table_full = f"{args.gold_db}.{args.gold_table}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.gold_db}")

    create_gold_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {gold_table_full} (
      card_id INT,
      disp_id INT,
      card_type STRING,
      issued_at TIMESTAMP,
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
      card_id,
      disp_id,
      card_type,
      issued_at,
      effective_from,
      effective_to,
      is_current,
      scd_version
    )
    SELECT
      s.card_id,
      s.disp_id,
      s.card_type,
      s.issued_at,
      current_timestamp()              AS effective_from,
      TIMESTAMP '9999-12-31 23:59:59' AS effective_to,
      TRUE                            AS is_current,
      1                               AS scd_version
    FROM {silver_table_full} s
    LEFT JOIN {gold_table_full} g
      ON s.card_id = g.card_id
     AND g.is_current = TRUE
    WHERE g.card_id IS NULL
    """
    spark.sql(insert_new_sql)

    insert_changed_sql = f"""
    INSERT INTO {gold_table_full}
    (
      card_id,
      disp_id,
      card_type,
      issued_at,
      effective_from,
      effective_to,
      is_current,
      scd_version
    )
    SELECT
      s.card_id,
      s.disp_id,
      s.card_type,
      s.issued_at,
      current_timestamp()              AS effective_from,
      TIMESTAMP '9999-12-31 23:59:59' AS effective_to,
      TRUE                            AS is_current,
      g.scd_version + 1               AS scd_version
    FROM {silver_table_full} s
    JOIN {gold_table_full} g
      ON s.card_id = g.card_id
     AND g.is_current = TRUE
    WHERE NOT (
      s.disp_id   <=> g.disp_id   AND
      s.card_type <=> g.card_type AND
      s.issued_at <=> g.issued_at
    )
    """
    spark.sql(insert_changed_sql)

    expire_old_sql = f"""
    UPDATE {gold_table_full} AS g
    SET
      g.effective_to = current_timestamp(),
      g.is_current   = FALSE
    WHERE g.is_current = TRUE
      AND g.card_id IN (
        SELECT g2.card_id
        FROM {silver_table_full} s
        JOIN {gold_table_full} g2
          ON s.card_id = g2.card_id
         AND g2.is_current = TRUE
        WHERE NOT (
          s.disp_id   <=> g2.disp_id   AND
          s.card_type <=> g2.card_type AND
          s.issued_at <=> g2.issued_at
        )
      )
    """
    spark.sql(expire_old_sql)

    spark.stop()


if __name__ == "__main__":
    main()
