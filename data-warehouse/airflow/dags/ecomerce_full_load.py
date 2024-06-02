import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging
logger = logging.getLogger(__name__)

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from common.helpers import ConstantsProvider, SourceConfigHandler

from ingest.full_load import full_source_to_HDFS, HDFS_LandingZone_to_Hive_Staging

source = ConstantsProvider.get_Ecomerce_source()

with DAG(
    f"{source}_full_load",
    default_args=ConstantsProvider.default_dag_args(),
    description=f"A full load data pipleline to ingest data from sources",
    schedule=timedelta(hours=24),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    source_config_handler = SourceConfigHandler(source=source, is_fullload=True)

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

        source_to_hdfs_task >> hdfs_to_hive_task