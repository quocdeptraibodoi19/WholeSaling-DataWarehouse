import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from datetime import datetime, timedelta

from airflow.models.dag import DAG

from common.helpers import ConstantsProvider

from ingest.Ecommerce_System.full_load import HR_to_HDFS, HDFS_LandingZone_to_Hive_Staging
from ingest.Ecommerce_System.delta_load import (
    update_delta_keys,
    delta_HR_to_HDFS,
    delta_HDFS_LandingZone_to_Hive_Staging,
    delete_detection_HR_HDFS,
    delete_detection_HDFS_LandingZone_to_Hive_Staging,
    reconciling_delta_delete_Hive_Staging,
    check_full_load_yet,
)

from ingest.task_generator import DeltaLoadTaskGenerator

with DAG(
    "delta_load_Ecom",
    default_args=ConstantsProvider.default_dag_args(),
    description=f"A delta load data pipleline to ingest data from {ConstantsProvider.get_Ecomerce_source()}",
    schedule=timedelta(hours=3),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ecom_task_generator = DeltaLoadTaskGenerator(
        dag=dag,
        source=ConstantsProvider.get_Ecomerce_source(),
        airflow_task_funcs={
            "HR_to_HDFS": HR_to_HDFS,
            "HDFS_LandingZone_to_Hive_Staging": HDFS_LandingZone_to_Hive_Staging,
            "update_delta_keys": update_delta_keys,
            "delta_HR_to_HDFS": delta_HR_to_HDFS,
            "delta_HDFS_LandingZone_to_Hive_Staging": delta_HDFS_LandingZone_to_Hive_Staging,
            "delete_detection_HR_HDFS": delete_detection_HR_HDFS,
            "delete_detection_HDFS_LandingZone_to_Hive_Staging": delete_detection_HDFS_LandingZone_to_Hive_Staging,
            "reconciling_delta_delete_Hive_Staging": reconciling_delta_delete_Hive_Staging,
            "check_full_load_yet": check_full_load_yet,
        },
    )
    ecom_task_generator.add_all_tasks()