#!/usr/bin/env python3
"""
PK/FK validation job for the Berka demo using Spark SQL.

This job mirrors the checks defined in scripts/validation/impala_pk_fk_checks.sql
but runs them via Spark with Hive support enabled.

Example:
  spark-submit scripts/validation/impala_pk_fk_checks_spark.py
"""

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run PK/FK validation checks for the Berka demo using Spark SQL."
    )
    parser.add_argument(
        "--bronze-db",
        default="bronze",
        help="Database/schema that holds the bronze tables. Default: bronze",
    )
    parser.add_argument(
        "--silver-db",
        default="silver",
        help="Database/schema that holds the silver tables. Default: silver",
    )
    parser.add_argument(
        "--gold-db",
        default="gold",
        help="Database/schema that holds the gold tables. Default: gold",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("berka_pk_fk_checks_spark_sql")
        .config("spark.security.credentials.hiveserver2.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )

    bronze = args.bronze_db
    silver = args.silver_db
    gold = args.gold_db

    checks = [
        # 0. Basic row counts
        (
            "row_counts",
            f"""
            SELECT '{bronze}.district_bronze' AS table_name, COUNT(*) AS row_count FROM {bronze}.district_bronze
            UNION ALL
            SELECT '{bronze}.client_bronze', COUNT(*) FROM {bronze}.client_bronze
            UNION ALL
            SELECT '{bronze}.account_bronze', COUNT(*) FROM {bronze}.account_bronze
            UNION ALL
            SELECT '{bronze}.disp_bronze', COUNT(*) FROM {bronze}.disp_bronze
            UNION ALL
            SELECT '{bronze}.card_bronze', COUNT(*) FROM {bronze}.card_bronze
            UNION ALL
            SELECT '{silver}.district_silver', COUNT(*) FROM {silver}.district_silver
            UNION ALL
            SELECT '{silver}.client_silver', COUNT(*) FROM {silver}.client_silver
            UNION ALL
            SELECT '{silver}.account_silver', COUNT(*) FROM {silver}.account_silver
            UNION ALL
            SELECT '{silver}.dq_account', COUNT(*) FROM {silver}.dq_account
            UNION ALL
            SELECT '{silver}.disp_silver', COUNT(*) FROM {silver}.disp_silver
            UNION ALL
            SELECT '{silver}.card_silver', COUNT(*) FROM {silver}.card_silver
            UNION ALL
            SELECT '{gold}.dim_district', COUNT(*) FROM {gold}.dim_district
            UNION ALL
            SELECT '{gold}.dim_client', COUNT(*) FROM {gold}.dim_client
            UNION ALL
            SELECT '{gold}.dim_account', COUNT(*) FROM {gold}.dim_account
            UNION ALL
            SELECT '{gold}.dim_disp', COUNT(*) FROM {gold}.dim_disp
            UNION ALL
            SELECT '{gold}.dim_card', COUNT(*) FROM {gold}.dim_card
            UNION ALL
            SELECT '{gold}.fact_loan', COUNT(*) FROM {gold}.fact_loan
            UNION ALL
            SELECT '{gold}.fact_order', COUNT(*) FROM {gold}.fact_order
            UNION ALL
            SELECT '{gold}.fact_trans', COUNT(*) FROM {gold}.fact_trans
            """,
        ),
        # 1. Bronze FK checks
        (
            "bronze_client_district_fk",
            f"""
            SELECT COUNT(*) AS bad_client_district_fk
            FROM {bronze}.client_bronze c
            LEFT JOIN {bronze}.district_bronze d
              ON CAST(c.district_id AS INT) = CAST(d.A1 AS INT)
            WHERE d.A1 IS NULL
            """,
        ),
        # Account bronze rows that pass the Silver filter predicate
        (
            "bronze_account_rows_passing_filter",
            f"""
            SELECT COUNT(*) AS eligible_for_account_silver
            FROM {bronze}.account_bronze b
            WHERE b.account_id  RLIKE '^[0-9]+$'
              AND b.district_id RLIKE '^[0-9]+$'
              AND b.frequency   IS NOT NULL
              AND b.frequency   <> ''
              AND b.created_date RLIKE '^[0-9]{{6}}$'
            """,
        ),
        (
            "bronze_account_district_fk",
            f"""
            SELECT COUNT(*) AS bad_account_district_fk
            FROM {bronze}.account_bronze a
            LEFT JOIN {bronze}.district_bronze d
              ON CAST(a.district_id AS INT) = CAST(d.A1 AS INT)
            WHERE d.A1 IS NULL
            """,
        ),
        (
            "bronze_disp_client_fk",
            f"""
            SELECT COUNT(*) AS bad_disp_client_fk
            FROM {bronze}.disp_bronze disp
            LEFT JOIN {bronze}.client_bronze c
              ON CAST(disp.client_id AS INT) = CAST(c.client_id AS INT)
            WHERE c.client_id IS NULL
            """,
        ),
        (
            "bronze_disp_account_fk",
            f"""
            SELECT COUNT(*) AS bad_disp_account_fk
            FROM {bronze}.disp_bronze disp
            LEFT JOIN {bronze}.account_bronze a
              ON CAST(disp.account_id AS INT) = CAST(a.account_id AS INT)
            WHERE a.account_id IS NULL
            """,
        ),
        (
            "bronze_card_disp_fk",
            f"""
            SELECT COUNT(*) AS bad_card_disp_fk
            FROM {bronze}.card_bronze card
            LEFT JOIN {bronze}.disp_bronze disp
              ON CAST(card.disp_id AS INT) = CAST(disp.disp_id AS INT)
            WHERE disp.disp_id IS NULL
            """,
        ),
        # 2. Silver dimensions PK/FK
        (
            "silver_district_pk",
            f"""
            SELECT COUNT(*) AS total_districts,
                   COUNT(DISTINCT district_id) AS distinct_district_ids
            FROM {silver}.district_silver
            """,
        ),
        (
            "silver_client_district_fk",
            f"""
            SELECT COUNT(*) AS bad_client_district_fk_silver
            FROM {silver}.client_silver c
            LEFT JOIN {silver}.district_silver d
              ON c.district_id = d.district_id
            WHERE d.district_id IS NULL
            """,
        ),
        (
            "silver_account_district_fk",
            f"""
            SELECT COUNT(*) AS bad_account_district_fk_silver
            FROM {silver}.account_silver a
            LEFT JOIN {silver}.district_silver d
              ON a.district_id = d.district_id
            WHERE d.district_id IS NULL
            """,
        ),
        (
            "silver_disp_client_fk",
            f"""
            SELECT COUNT(*) AS bad_disp_client_fk_silver
            FROM {silver}.disp_silver disp
            LEFT JOIN {silver}.client_silver c
              ON disp.client_id = c.client_id
            WHERE c.client_id IS NULL
            """,
        ),
        (
            "silver_disp_account_fk",
            f"""
            SELECT COUNT(*) AS bad_disp_account_fk_silver
            FROM {silver}.disp_silver disp
            LEFT JOIN {silver}.account_silver a
              ON disp.account_id = a.account_id
            WHERE a.account_id IS NULL
            """,
        ),
        (
            "silver_card_disp_fk",
            f"""
            SELECT COUNT(*) AS bad_card_disp_fk_silver
            FROM {silver}.card_silver card
            LEFT JOIN {silver}.disp_silver disp
              ON card.disp_id = disp.disp_id
            WHERE disp.disp_id IS NULL
            """,
        ),
        # 3. Gold dimensions PK/FK (is_current)
        (
            "gold_dim_client_district_fk",
            f"""
            SELECT COUNT(*) AS bad_dim_client_district_fk
            FROM {gold}.dim_client c
            LEFT JOIN {gold}.dim_district d
              ON c.district_id = d.district_id
            WHERE d.district_id IS NULL
              AND c.is_current = TRUE
            """,
        ),
        (
            "gold_dim_account_district_fk",
            f"""
            SELECT COUNT(*) AS bad_dim_account_district_fk
            FROM {gold}.dim_account a
            LEFT JOIN {gold}.dim_district d
              ON a.district_id = d.district_id
            WHERE d.district_id IS NULL
              AND a.is_current = TRUE
            """,
        ),
        (
            "gold_dim_disp_client_fk",
            f"""
            SELECT COUNT(*) AS bad_dim_disp_client_fk
            FROM {gold}.dim_disp disp
            LEFT JOIN {gold}.dim_client c
              ON disp.client_id = c.client_id AND c.is_current = TRUE
            WHERE c.client_id IS NULL
              AND disp.is_current = TRUE
            """,
        ),
        (
            "gold_dim_disp_account_fk",
            f"""
            SELECT COUNT(*) AS bad_dim_disp_account_fk
            FROM {gold}.dim_disp disp
            LEFT JOIN {gold}.dim_account a
              ON disp.account_id = a.account_id AND a.is_current = TRUE
            WHERE a.account_id IS NULL
              AND disp.is_current = TRUE
            """,
        ),
        (
            "gold_dim_card_disp_fk",
            f"""
            SELECT COUNT(*) AS bad_dim_card_disp_fk
            FROM {gold}.dim_card card
            LEFT JOIN {gold}.dim_disp disp
              ON card.disp_id = disp.disp_id AND disp.is_current = TRUE
            WHERE disp.disp_id IS NULL
              AND card.is_current = TRUE
            """,
        ),
        # 4. Fact vs dimensions
        (
            "gold_fact_loan_fks",
            f"""
            SELECT 'fact_loan_account' AS check_name, COUNT(*) AS bad_fk
            FROM {gold}.fact_loan f
            LEFT JOIN {gold}.dim_account a
              ON f.account_id = a.account_id AND a.is_current = TRUE
            WHERE a.account_id IS NULL
            UNION ALL
            SELECT 'fact_loan_client', COUNT(*)
            FROM {gold}.fact_loan f
            LEFT JOIN {gold}.dim_client c
              ON f.client_id = c.client_id AND c.is_current = TRUE
            WHERE c.client_id IS NULL
            UNION ALL
            SELECT 'fact_loan_disp', COUNT(*)
            FROM {gold}.fact_loan f
            LEFT JOIN {gold}.dim_disp disp
              ON f.disp_id = disp.disp_id AND disp.is_current = TRUE
            WHERE disp.disp_id IS NULL
            """,
        ),
        (
            "gold_fact_order_fks",
            f"""
            SELECT 'fact_order_account' AS check_name, COUNT(*) AS bad_fk
            FROM {gold}.fact_order f
            LEFT JOIN {gold}.dim_account a
              ON f.account_id = a.account_id AND a.is_current = TRUE
            WHERE a.account_id IS NULL
            UNION ALL
            SELECT 'fact_order_client', COUNT(*)
            FROM {gold}.fact_order f
            LEFT JOIN {gold}.dim_client c
              ON f.client_id = c.client_id AND c.is_current = TRUE
            WHERE c.client_id IS NULL
            UNION ALL
            SELECT 'fact_order_disp', COUNT(*)
            FROM {gold}.fact_order f
            LEFT JOIN {gold}.dim_disp disp
              ON f.disp_id = disp.disp_id AND disp.is_current = TRUE
            WHERE disp.disp_id IS NULL
            """,
        ),
        (
            "gold_fact_trans_fks",
            f"""
            SELECT 'fact_trans_account' AS check_name, COUNT(*) AS bad_fk
            FROM {gold}.fact_trans f
            LEFT JOIN {gold}.dim_account a
              ON f.account_id = a.account_id AND a.is_current = TRUE
            WHERE a.account_id IS NULL
            UNION ALL
            SELECT 'fact_trans_client', COUNT(*)
            FROM {gold}.fact_trans f
            LEFT JOIN {gold}.dim_client c
              ON f.client_id = c.client_id AND c.is_current = TRUE
            WHERE c.client_id IS NULL
            UNION ALL
            SELECT 'fact_trans_disp', COUNT(*)
            FROM {gold}.fact_trans f
            LEFT JOIN {gold}.dim_disp disp
              ON f.disp_id = disp.disp_id AND disp.is_current = TRUE
            WHERE disp.disp_id IS NULL
            """,
        ),
    ]

    for name, sql in checks:
        print(f"\n===== CHECK: {name} =====")
        spark.sql(sql).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
