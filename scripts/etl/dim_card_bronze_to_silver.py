#!/usr/bin/env python3
"""
Bronze → Silver job for the `card` dimension.
"""

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bronze → Silver Spark SQL job for the card dimension."
    )
    parser.add_argument(
        "--bronze-db",
        default="bronze",
        help="Database/schema that holds the bronze card table. Default: bronze",
    )
    parser.add_argument(
        "--silver-db",
        default="silver",
        help="Database/schema for the silver card table. Default: silver",
    )
    parser.add_argument(
        "--bronze-table",
        default="card_bronze",
        help="Bronze table name for card data. Default: card_bronze",
    )
    parser.add_argument(
        "--silver-table",
        default="card_silver",
        help="Silver table name for card data. Default: card_silver",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("dim_card_bronze_to_silver_sql")
        .enableHiveSupport()
        .getOrCreate()
    )

    bronze_table_full = f"{args.bronze_db}.{args.bronze_table}"
    silver_table_full = f"{args.silver_db}.{args.silver_table}"
    dq_table_full = f"{args.silver_db}.dq_card"

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {dq_table_full} (
          card_id   STRING,
          disp_id   STRING,
          type      STRING,
          issued    STRING,
          dq_date   DATE,
          dq_reason STRING
        )
        USING parquet
        TBLPROPERTIES ('parquet.compression' = 'snappy')
        """
    )

    dq_insert_sql = f"""
    INSERT INTO {dq_table_full}
    (
      card_id,
      disp_id,
      type,
      issued,
      dq_date,
      dq_reason
    )
    SELECT
      card_id,
      disp_id,
      type,
      issued,
      current_date AS dq_date,
      'Invalid card/disp identifiers, type, or issued format' AS dq_reason
    FROM {bronze_table_full}
    WHERE NOT (
      card_id RLIKE '^[0-9]+$' AND
      disp_id RLIKE '^[0-9]+$' AND
      type IS NOT NULL AND type <> '' AND
      issued RLIKE '^[0-9]{6} '
    )
    """
    spark.sql(dq_insert_sql)

    create_silver_sql = f"""
    CREATE OR REPLACE TABLE {silver_table_full}
    USING parquet
    TBLPROPERTIES ('parquet.compression' = 'snappy') AS
    SELECT
      CAST(card_id AS INT)   AS card_id,
      CAST(disp_id AS INT)   AS disp_id,
      type                   AS card_type,
      TO_TIMESTAMP(issued, 'yyMMdd HH:mm:ss') AS issued_at
    FROM {bronze_table_full}
    WHERE card_id RLIKE '^[0-9]+$'
      AND disp_id RLIKE '^[0-9]+$'
      AND type IS NOT NULL
      AND type <> ''
      AND issued RLIKE '^[0-9]{6} '
    """

    spark.sql(create_silver_sql)

    spark.stop()


if __name__ == "__main__":
    main()
