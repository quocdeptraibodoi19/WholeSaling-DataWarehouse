import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging

logger = logging.getLogger(__name__)

from datetime import datetime, timedelta
import ast

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from common.helpers import ConstantsProvider, SourceConfigHandler

from ingest.full_load import full_source_to_HDFS, HDFS_LandingZone_to_Hive_Staging

source = ConstantsProvider.get_Product_source()
source_config_handler = SourceConfigHandler(source=source, is_fullload=True)
default_table_options = [
    table_config.get("table")
    for table_config in source_config_handler.get_tables_configs()
]
extend_table_options = default_table_options + [
    ConstantsProvider.get_airflow_all_tables_option()
]


def branching_tasks(chosen_tables_param: str, default_tables: list[str]) -> list[str]:
    chosen_tables = ast.literal_eval(chosen_tables_param)

    if (
        len(chosen_tables) == 0
        or ConstantsProvider.get_airflow_all_tables_option() in chosen_tables
    ):
        chosen_tables = default_tables

    considered_tasks = []
    for table in chosen_tables:
        task_identifier = f"{table}_from_{source}"
        considered_tasks.append(f"{task_identifier}_full_load_source_to_HDFS")

    return considered_tasks


with DAG(
    f"{source}_full_load",
    default_args=ConstantsProvider.default_dag_args(),
    description=f"A full load data pipleline to ingest data from sources",
    schedule=timedelta(hours=24),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "considered_tables": Param(
            [],
            type="array",
            title="Considered Tables",
            examples=extend_table_options,
            description="Please select the tables you want to replicate. By default, all tables are considered.",
        )
    },
) as dag:

    branching_tables_task = BranchPythonOperator(
        task_id=f"branching_table_in_{source}_full_load",
        python_callable=branching_tasks,
        op_kwargs={
            "chosen_tables_param": "{{ params.considered_tables }}",
            "default_tables": default_table_options,
        },
        dag=dag,
    )

    for table_config in source_config_handler.get_tables_configs():
        table = table_config.get("table")
        task_identifier = f"{table}_from_{source}"

        source_to_hdfs_task = PythonOperator(
            task_id=f"{task_identifier}_full_load_source_to_HDFS",
            python_callable=full_source_to_HDFS,
            op_kwargs={
                "table_config": table_config,
                "source": source,
                "logger": logger,
            },
            dag=dag,
        )

        hdfs_to_hive_task = PythonOperator(
            task_id=f"{task_identifier}_full_load_HDFS_to_Hive",
            python_callable=HDFS_LandingZone_to_Hive_Staging,
            op_kwargs={
                "table": table_config.get("table"),
                "source": source,
                "logger": logger,
            },
            dag=dag,
        )

        branching_tables_task >> source_to_hdfs_task >> hdfs_to_hive_task
