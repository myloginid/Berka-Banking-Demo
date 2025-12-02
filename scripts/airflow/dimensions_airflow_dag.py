"""
Airflow DAG for running the Berka dimension pipeline in CDE.

The DAG reflects the logical flow documented in the README
("Airflow‑Style Dimension DAG"), ignoring the NiFi bronze load
step (assumed to have already populated the bronze layer).

Task graph (Spark jobs are pre-created in CDE):

  dim_district_bronze_to_silver
    -> dim_district_silver_to_gold
        -> dim_client_bronze_to_silver
            -> dim_client_silver_to_gold
        -> dim_account_bronze_to_silver
            -> dim_account_silver_to_gold
        -> dim_disp_bronze_to_silver
            -> dim_disp_silver_to_gold
                -> dim_card_bronze_to_silver
                    -> dim_card_silver_to_gold
"""

from datetime import datetime

from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator


default_args = {
    "owner": "manishm",
    "depends_on_past": False,
}


with DAG(
    dag_id="berka_dimensions_pipeline",
    description="Run Berka dimension jobs in the correct dependency order on CDE",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    # District: bronze -> silver -> gold
    dim_district_bronze_to_silver = CDEJobRunOperator(
        task_id="dim_district_bronze_to_silver",
        job_name="dim-district-bronze-to-silver",
    )

    dim_district_silver_to_gold = CDEJobRunOperator(
        task_id="dim_district_silver_to_gold",
        job_name="dim-district-silver-to-gold",
    )

    dim_district_bronze_to_silver >> dim_district_silver_to_gold

    # Client: bronze -> gold, depends on district gold
    dim_client_bronze_to_silver = CDEJobRunOperator(
        task_id="dim_client_bronze_to_silver",
        job_name="dim-client-bronze-to-silver",
    )

    dim_client_silver_to_gold = CDEJobRunOperator(
        task_id="dim_client_silver_to_gold",
        job_name="dim-client-silver-to-gold",
    )

    dim_district_silver_to_gold >> dim_client_bronze_to_silver >> dim_client_silver_to_gold

    # Account: bronze -> gold, depends on district gold
    dim_account_bronze_to_silver = CDEJobRunOperator(
        task_id="dim_account_bronze_to_silver",
        job_name="dim-account-bronze-to-silver",
    )

    dim_account_silver_to_gold = CDEJobRunOperator(
        task_id="dim_account_silver_to_gold",
        job_name="dim-account-silver-to-gold",
    )

    dim_district_silver_to_gold >> dim_account_bronze_to_silver >> dim_account_silver_to_gold

    # Disp: bronze -> gold, requires client, account and district
    dim_disp_bronze_to_silver = CDEJobRunOperator(
        task_id="dim_disp_bronze_to_silver",
        job_name="dim-disp-bronze-to-silver",
    )

    dim_disp_silver_to_gold = CDEJobRunOperator(
        task_id="dim_disp_silver_to_gold",
        job_name="dim-disp-silver-to-gold",
    )

    [
        dim_district_silver_to_gold,
        dim_client_silver_to_gold,
        dim_account_silver_to_gold,
    ] >> dim_disp_bronze_to_silver >> dim_disp_silver_to_gold

    # Card: bronze -> gold, requires disp
    dim_card_bronze_to_silver = CDEJobRunOperator(
        task_id="dim_card_bronze_to_silver",
        job_name="dim-card-bronze-to-silver",
    )

    dim_card_silver_to_gold = CDEJobRunOperator(
        task_id="dim_card_silver_to_gold",
        job_name="dim-card-silver-to-gold",
    )

    dim_disp_silver_to_gold >> dim_card_bronze_to_silver >> dim_card_silver_to_gold
