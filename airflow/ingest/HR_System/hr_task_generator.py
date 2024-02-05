import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging

from common.helpers import ConstantsProvider

import yaml

from airflow import DAG
from airflow.operators.python import PythonOperator

from .full_load import HR_to_HDFS, HDFS_LandingZone_to_Hive_Staging


class HRTaskGenerator:
    def __init__(self, dag: DAG) -> None:
        self.logger = logging.getLogger(__name__)
        self.dag = dag
        self.source = ConstantsProvider.get_HR_source()
        with open(
            ConstantsProvider.config_file_path(
                source=ConstantsProvider.get_HR_source()
            ),
            "r",
        ) as file:
            self.config = yaml.load(file, yaml.Loader)

    def add_all_full_load_tasks(self):
        tables = self.config.get("full_load")

        for table in tables:
            t1 = self.create_python_run_task(
                task_id=f"ingest_{table}_from_{ConstantsProvider.get_HR_source()}",
                python_callable=HR_to_HDFS,
                op_kwargs={
                    "table": table,
                    "source": self.source,
                    "logger": self.logger,
                },
            )

            t2 = self.create_python_run_task(
                task_id=f"ingest_{table}_from_HDFS_to_Hive",
                python_callable=HDFS_LandingZone_to_Hive_Staging,
                op_kwargs={
                    "table": table,
                    "source": self.source,
                    "logger": self.logger,
                },
            )

            t1 >> t2

    def create_python_run_task(
        self, task_id: str, python_callable: callable, op_kwargs: dict
    ) -> PythonOperator:
        return PythonOperator(
            task_id=task_id,
            python_callable=python_callable,
            op_kwargs=op_kwargs,
            dag=self.dag,
        )
