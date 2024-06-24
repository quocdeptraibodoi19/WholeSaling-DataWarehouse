import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging

logger = logging.getLogger(__name__)

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from common.helpers import ConstantsProvider, SourceConfigHandler, branching_tasks

from ingest.full_load import full_source_to_HDFS, HDFS_LandingZone_to_Hive_Staging

source = ConstantsProvider.get_WholeSaling_source()
source_config_handler = SourceConfigHandler(source=source, is_fullload=True)
default_table_options = [
    table_config.get("table")
    for table_config in source_config_handler.get_tables_configs()
]
extend_table_options = default_table_options + [
    ConstantsProvider.get_airflow_all_tables_option()
]


with DAG(
    f"{source}_full_load",
    default_args=ConstantsProvider.default_dag_args(),
    description=f"A full load data pipleline to ingest data from sources",
    schedule='0 4 * * 0', # Sundays at 4 AM
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

    for table_config in source_config_handler.get_tables_configs():
        table = table_config.get("table")
        task_identifier = f"{table}_from_{source}"

        branching_tables_task = ShortCircuitOperator(
            task_id=f"branchin_table_{source}_{table}",
            python_callable=branching_tasks,
            op_kwargs={
                "table": table,
                "chosen_tables_param": "{{ params.considered_tables }}",
            },
            dag=dag,
        )

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
