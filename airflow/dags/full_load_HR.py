import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from common.helpers import ConstantsProvider
from ingest.HR_System.full_load import HR_to_HDFS, HDFS_LandingZone_to_Hive_Staging

import yaml

with DAG(
    "full_load_HR",
    default_args=ConstantsProvider.default_dag_args(),
    description="This is the first DAG to be executed",
    schedule=timedelta(hours=3),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    with open(
        ConstantsProvider.config_file_path(source=ConstantsProvider.get_HR_source()),
        "r",
    ) as file:
        tables = yaml.load(file, yaml.Loader).get("full_load")

    for table in tables:
        t1 = PythonOperator(
            task_id=f"ingest_{table}_from_{ConstantsProvider.get_HR_source()}",
            python_callable=HR_to_HDFS,
            op_kwargs={
                "table": table,
                "source": ConstantsProvider.get_HR_source(),
            },
            dag=dag,
        )

        t2 = PythonOperator(
            task_id=f"ingest_{table}_from_HDFS_to_Hive",
            python_callable=HDFS_LandingZone_to_Hive_Staging,
            op_kwargs={
                "table": table,
                "source": ConstantsProvider.get_HR_source(),
            },
            dag=dag,
        )

        t1 >> t2
