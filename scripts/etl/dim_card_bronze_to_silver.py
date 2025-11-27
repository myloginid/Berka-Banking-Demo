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
    """

    spark.sql(create_silver_sql)

    spark.stop()


if __name__ == "__main__":
    main()
