#!/usr/bin/env python3
"""
Silver → Gold SCD Type 2 job for the `client` dimension using Iceberg tables.

Reads cleaned client records from the silver layer and maintains a
dimensional SCD2 table in the gold layer, implemented purely with Spark SQL.
"""

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Silver → Gold Spark SQL job for the client dimension (SCD Type 2, Iceberg)."
    )
    parser.add_argument(
        "--silver-db",
        default="silver",
        help="Database/schema that holds the silver client table. Default: silver",
    )
    parser.add_argument(
        "--gold-db",
        default="gold",
        help="Database/schema for the gold dim_client table. Default: gold",
    )
    parser.add_argument(
        "--silver-table",
        default="client_silver",
        help="Silver table name for client data. Default: client_silver",
    )
    parser.add_argument(
        "--gold-table",
        default="dim_client",
        help="Gold dimension table name (Iceberg). Default: dim_client",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("dim_client_silver_to_gold_scd2_sql")
        .config("spark.security.credentials.hiveserver2.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )

    silver_table_full = f"{args.silver_db}.{args.silver_table}"
    gold_table_full = f"{args.gold_db}.{args.gold_table}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.gold_db}")

    create_gold_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {gold_table_full} (
      client_id INT,
      birth_number STRING,
      district_id INT,
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
      client_id,
      birth_number,
      district_id,
      effective_from,
      effective_to,
      is_current,
      scd_version
    )
    SELECT
      s.client_id,
      s.birth_number,
      s.district_id,
      current_timestamp()                         AS effective_from,
      TIMESTAMP '9999-12-31 23:59:59'            AS effective_to,
      TRUE                                       AS is_current,
      1                                          AS scd_version
    FROM {silver_table_full} s
    LEFT JOIN {gold_table_full} g
      ON s.client_id = g.client_id
     AND g.is_current = TRUE
    WHERE g.client_id IS NULL
    """
    spark.sql(insert_new_sql)

    insert_changed_sql = f"""
    INSERT INTO {gold_table_full}
    (
      client_id,
      birth_number,
      district_id,
      effective_from,
      effective_to,
      is_current,
      scd_version
    )
    SELECT
      s.client_id,
      s.birth_number,
      s.district_id,
      current_timestamp()              AS effective_from,
      TIMESTAMP '9999-12-31 23:59:59' AS effective_to,
      TRUE                            AS is_current,
      g.scd_version + 1               AS scd_version
    FROM {silver_table_full} s
    JOIN {gold_table_full} g
      ON s.client_id = g.client_id
     AND g.is_current = TRUE
    WHERE NOT (
      s.birth_number <=> g.birth_number AND
      s.district_id  <=> g.district_id
    )
    """
    spark.sql(insert_changed_sql)

    expire_old_sql = f"""
    UPDATE {gold_table_full} AS g
    SET
      g.effective_to = current_timestamp(),
      g.is_current   = FALSE
    WHERE g.is_current = TRUE
      AND g.client_id IN (
        SELECT g2.client_id
        FROM {silver_table_full} s
        JOIN {gold_table_full} g2
          ON s.client_id = g2.client_id
         AND g2.is_current = TRUE
        WHERE NOT (
          s.birth_number <=> g2.birth_number AND
          s.district_id  <=> g2.district_id
        )
      )
    """
    spark.sql(expire_old_sql)

    spark.stop()


if __name__ == "__main__":
    main()
