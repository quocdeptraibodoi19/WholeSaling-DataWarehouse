import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging

logger = logging.getLogger(__name__)

from datetime import datetime, timedelta

from common.helpers import ConstantsProvider

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

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


with DAG(
    f"data_warehouse_transformation",
    default_args=ConstantsProvider.default_dag_args(),
    description=f"A tranformation part (T) in the ELT pipeline",
    schedule=timedelta(hours=24),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    for model in dbt_selected_models:
        task_id = f"dbt_build_{model}"
        dbt_command = f"cd {DBT_PROJECT_DIR} && dbt build --select +{model}"
        build_task = BashOperator(
            task_id=task_id,
            bash_command=dbt_command,
            dag=dag,
        )
        # Due to the resource limitation, set the tasks to run sequentially
        if model != dbt_selected_models[0]:
            build_task.set_upstream(previous_task)

        previous_task = build_task
