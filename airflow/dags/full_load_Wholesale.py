import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from datetime import datetime, timedelta

from airflow.models.dag import DAG

from common.helpers import ConstantsProvider

from ingest.task_generator import FullLoadTaskGenerator

from ingest.WholeSale_System.full_load import HR_to_HDFS, HDFS_LandingZone_to_Hive_Staging

with DAG(
    "full_load_Wholesale",
    default_args=ConstantsProvider.default_dag_args(),
    description=f"A full load data pipleline to ingest data from {ConstantsProvider.get_WholeSaling_source()}",
    schedule=timedelta(hours=24),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    wholesale_task_generator = FullLoadTaskGenerator(
        dag=dag,
        source=ConstantsProvider.get_WholeSaling_source(),
        airflow_task_funcs={
            "HR_to_HDFS": HR_to_HDFS,
            "HDFS_LandingZone_to_Hive_Staging": HDFS_LandingZone_to_Hive_Staging,
        },
    )
    wholesale_task_generator.add_all_tasks()
