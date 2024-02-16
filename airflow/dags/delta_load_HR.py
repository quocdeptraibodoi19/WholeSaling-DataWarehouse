import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from datetime import datetime, timedelta

from airflow.models.dag import DAG

from common.helpers import ConstantsProvider

from ingest.HR_System.full_load import HR_to_HDFS, HDFS_LandingZone_to_Hive_Staging
from ingest.HR_System.delta_load import (
    update_delta_keys,
    delta_HR_to_HDFS,
    delta_HDFS_LandingZone_to_Hive_Staging,
    delete_detection_HR_HDFS,
    delete_detection_HDFS_LandingZone_to_Hive_Staging,
    reconciling_delta_delete_Hive_Staging,
)

from airflow.operators.python import PythonOperator

import logging

with DAG(
    "delta_load_HR",
    default_args=ConstantsProvider.default_dag_args(),
    description=f"A delta load data pipleline to ingest data from {ConstantsProvider.get_HR_source()}",
    schedule=timedelta(hours=3),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    table = "Stakeholder"
    source = ConstantsProvider.get_HR_source()
    logger = logging.getLogger(__name__)
    table_config = {
        "table": "Stakeholder",
        "primary_keys": ["StackHolderID"],
        "delta_keys": ["ModifiedDate"],
        "custom_load_sql": "Select * from Stackholder where (ModifiedDate > {ModifiedDate})",
        "delta_load_hql": "SELECT MAX(MODIFIEDDATE) AS MODIFIEDDATE FROM {hive_table}",
    }

    t1 = PythonOperator(
        task_id=f"delta_ingest_{table}_from_{ConstantsProvider.get_HR_source()}",
        python_callable=HR_to_HDFS,
        op_kwargs={
            "table": table,
            "source": source,
            "logger": logger,
        },
        dag=dag,
    )

    t2 = PythonOperator(
        task_id=f"delta_ingest_{table}_from_HDFS_to_Hive",
        python_callable=HDFS_LandingZone_to_Hive_Staging,
        op_kwargs={
            "table": table,
            "source": source,
            "logger": logger,
        },
        dag=dag,
    )

    t3 = PythonOperator(
        task_id=f"update_detla_key_table_{source.lower()}_{table.lower()}_Hive",
        python_callable=update_delta_keys,
        op_kwargs={
            "source": source,
            "logger": logger,
            "table_config": table_config,
        },
    )

    t1 >> t2 >> t3

    # t4 = PythonOperator(
    #     task_id=f"delete_detection_{table.lower()}_{source.lower()}_HDFS",
    #     python_callable=delete_detection_HR_HDFS,
    #     op_kwargs={
    #         "source": source,
    #         "logger": logger,
    #         "table_config": table_config,
    #     },
    # )

    # t5 = PythonOperator(
    #     task_id=f"delete_detection_{table.lower()}_HDFS_Hive",
    #     python_callable=delete_detection_HDFS_LandingZone_to_Hive_Staging,
    #     op_kwargs={
    #         "source": source,
    #         "logger": logger,
    #         "table_config": table_config,
    #     },
    # )

    # t6 = PythonOperator(
    #     task_id=f"delete_detection_{table.lower()}_Reconcile_Hive",
    #     python_callable=reconciling_delta_delete_Hive_Staging,
    #     op_kwargs={
    #         "source": source,
    #         "logger": logger,
    #         "table_config": table_config,
    #         "mode": "recocile_delete",
    #     },
    # )

    # t7 = PythonOperator(
    #     task_id=f"delta_load_{table.lower()}_{source.lower()}_HDFS",
    #     python_callable=delta_HR_to_HDFS,
    #     op_kwargs={
    #         "source": source,
    #         "logger": logger,
    #         "table_config": table_config,
    #     },
    # )

    # t8 = PythonOperator(
    #     task_id=f"delta_load_{table.lower()}_HDFS_Hive",
    #     python_callable=delta_HDFS_LandingZone_to_Hive_Staging,
    #     op_kwargs={
    #         "source": source,
    #         "logger": logger,
    #         "table_config": table_config,
    #     },
    # )

    # t9 = PythonOperator(
    #     task_id=f"delta_load_{table.lower()}_Reconcile_Hive",
    #     python_callable=reconciling_delta_delete_Hive_Staging,
    #     op_kwargs={
    #         "source": source,
    #         "logger": logger,
    #         "table_config": table_config,
    #         "mode": "delta",
    #     },
    # )

    # t4 >> t5 >> t6 >> t7 >> t8 >> t9