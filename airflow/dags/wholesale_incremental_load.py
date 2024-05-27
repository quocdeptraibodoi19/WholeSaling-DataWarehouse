import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging
logger = logging.getLogger(__name__)

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.edgemodifier import Label
from airflow.operators.empty import EmptyOperator

from common.helpers import ConstantsProvider, SourceConfigHandler

from ingest.delta_load import (
    delta_source_to_HDFS, 
    delta_HDFS_LandingZone_to_Hive_Staging,
    delete_source_to_HDFS,
    delete_HDFS_LandingZone_to_Hive_Staging,
    reconciling_delta_delete_Hive_Staging,
    update_LSET,
    check_full_load_yet,
)

from ingest.full_load import full_source_to_HDFS, HDFS_LandingZone_to_Hive_Staging


def full_or_incremental_branch_detector(source: str, table: str, true_task: str, false_task: str):
    logger.info(
        f"Detecting what is the next task of the table {table} to be executed: {true_task} or {false_task} ... "
    )

    if check_full_load_yet(
        logger=logger,
        source=source,
        table=table,
    ):
        task_to_implemented = true_task
    else:
        task_to_implemented = false_task

    logger.info(f"The task to be excuted is {task_to_implemented}")

    return task_to_implemented


source = ConstantsProvider.get_WholeSaling_source()

with DAG(
    f"{source}_incremental_load",
    default_args=ConstantsProvider.default_dag_args(),
    description=f"An incremental load data pipleline to ingest data from sources",
    schedule=timedelta(hours=24),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    source_config_handler = SourceConfigHandler(source=source, is_fullload=False)

    for table_config in source_config_handler.get_tables_configs():
        table = table_config.get("table")
        task_identifier = f"{table}_from_{source}"

        incremental_dummy_task = EmptyOperator(
            task_id=f"{task_identifier}_dummy_stage_incremental_load",
            dag=dag,
        )

        full_source_to_hdfs_task = PythonOperator(
            task_id=f"{task_identifier}_full_load_source_to_HDFS",
            python_callable=full_source_to_HDFS,
            op_kwargs={
                "table_config": table_config,
                "source": source,
                "logger": logger,
                "is_delta_used": True,
            },
            dag=dag,
        )

        full_hdfs_to_hive_task = PythonOperator(
            task_id=f"{task_identifier}_full_load_HDFS_to_Hive",
            python_callable=HDFS_LandingZone_to_Hive_Staging,
            op_kwargs={
                "table": table,
                "source": source,
                "logger": logger,
            },
            dag=dag,
        )

        delta_source_to_hdfs_task = PythonOperator(
            task_id=f"{task_identifier}_incremental_load_delta_to_HDFS",
            python_callable=delta_source_to_HDFS,
            op_kwargs={
                "table_config": table_config,
                "source": source,
                "logger": logger,
            },
            dag=dag,
        )

        delta_hdfs_to_hive_task = PythonOperator(
            task_id=f"{task_identifier}_incremental_load_delta_HDFS_to_hive",
            python_callable=delta_HDFS_LandingZone_to_Hive_Staging,
            op_kwargs={
                "table_config": table_config,
                "source": source,
                "logger": logger,
            },
            dag=dag,
        )
        
        delete_source_to_hdfs_task = PythonOperator(
            task_id=f"{task_identifier}_incremental_load_delete_to_HDFS",
            python_callable=delete_source_to_HDFS,
            op_kwargs={
                "table_config": table_config,
                "source": source,
                "logger": logger,
            },
            dag=dag,
        )

        delete_hdfs_to_hive_task = PythonOperator(
            task_id=f"{task_identifier}_incremental_load_delete_HDFS_to_hive",
            python_callable=delete_HDFS_LandingZone_to_Hive_Staging,
            op_kwargs={
                "table_config": table_config,
                "source": source,
                "logger": logger,
            },
            dag=dag,
        )
        
        reconcile_delta_delete_task = PythonOperator(
            task_id=f"{task_identifier}_reconcile_HDFS_delta",
            python_callable=reconciling_delta_delete_Hive_Staging,
            op_kwargs={
                "table_config": table_config,
                "source": source,
                "logger": logger,
            },
            dag=dag,
        )
        
        update_LSET_task = PythonOperator(
            task_id=f"{task_identifier}_update_LSET",
            python_callable=update_LSET,
            op_kwargs={
                "table_config": table_config,
                "source": source,
                "logger": logger,
            },
            dag=dag,
            trigger_rule="none_failed",
        )

        full_incremental_branch_task = BranchPythonOperator(
            task_id=f"{task_identifier}_full_incremental_branching",
            python_callable=full_or_incremental_branch_detector,
            op_kwargs={
                "source": source,
                "table": table,
                "true_task": f"{task_identifier}_dummy_stage_incremental_load",
                "false_task": f"{task_identifier}_full_load_source_to_HDFS",
            },
            dag=dag,
        )

        full_incremental_branch_task >> Label("full_load") >> full_source_to_hdfs_task
        full_incremental_branch_task >> Label("incremental_load") >> incremental_dummy_task

        # Full load flow
        full_source_to_hdfs_task >> full_hdfs_to_hive_task

        # Incremental load flow
        incremental_dummy_task >> [delta_source_to_hdfs_task, delete_source_to_hdfs_task]
        
        delta_source_to_hdfs_task >> delta_hdfs_to_hive_task
        delete_source_to_hdfs_task >> delete_hdfs_to_hive_task

        [delta_hdfs_to_hive_task, delete_hdfs_to_hive_task] >> reconcile_delta_delete_task

        # Update LSET
        [full_hdfs_to_hive_task, reconcile_delta_delete_task] >> update_LSET_task
