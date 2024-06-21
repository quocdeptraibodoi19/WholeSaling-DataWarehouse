import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging

logger = logging.getLogger(__name__)

from datetime import datetime, timedelta

from common.helpers import ConstantsProvider

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from ingest.data_firewall import (
    compare_and_identify_ambiguous_records,
    move_ambiguous_records_to_DQ_table,
)

import subprocess


def find_dbt_executable_path():
    # Run the command to locate the dbt executable
    result = subprocess.run(["which", "dbt"], capture_output=True, text=True)

    # Check if the command was successful
    if result.returncode == 0:
        # Extract the path from the output
        dbt_executable_path = result.stdout.strip()
        return dbt_executable_path
    else:
        # Command failed, raise an exception or handle the error accordingly
        raise Exception("Failed to find dbt executable")


dbt_executable_path = find_dbt_executable_path()
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
dbt_selected_models = [
    "dimAddress",
    "dimCustomer",
    "dimDate",
    "dimOrderStatus",
    "dimProduct",
    "dimPromotion",
    "dimTime",
    "fctSales",
]


def ambiguous_recods_identification(
    hive_tables: list[str], primary_key: str, ambiguous_key: str, logger: logging.Logger
):
    ambiguous_records = compare_and_identify_ambiguous_records(
        hive_tables=hive_tables,
        primary_key=primary_key,
        ambiguous_key=ambiguous_key,
        logger=logger,
    )
    move_ambiguous_records_to_DQ_table(logger, [ambiguous_records])
    if len(ambiguous_records) != 0:
        raise Exception (
            f"There are {len(ambiguous_records)} ambiguous records. Please confirm it!"
        )


with DAG(
    f"data_warehouse_transformation",
    default_args=ConstantsProvider.default_dag_args(),
    description=f"A tranformation part (T) in the ELT pipeline",
    schedule=timedelta(hours=24),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    data_fire_wall = PythonOperator(
        task_id="data_fire_wall",
        python_callable=ambiguous_recods_identification,
        op_kwargs={
            "hive_tables": [
                "product_management_platform_product",
                "ecomerce_product",
                "wholesale_system_product",
            ],
            "primary_key": "productid",
            "ambiguous_key": "name",
            "logger": logger,
        },
        dag=dag,
    )

    previous_task = BashOperator(
        task_id=f"dbt_snapshot",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot",
        # bash_command=f"cd {DBT_PROJECT_DIR}",
        dag=dag,
    )

    data_fire_wall >> previous_task

    for model in dbt_selected_models:
        task_id = f"dbt_build_{model}"
        dbt_command = f"cd {DBT_PROJECT_DIR} && dbt build --select +{model}"
        build_task = BashOperator(
            task_id=task_id,
            bash_command=dbt_command,
            dag=dag,
        )
        # Due to the resource limitation, set the tasks to run sequentially
        build_task.set_upstream(previous_task)
        previous_task = build_task
