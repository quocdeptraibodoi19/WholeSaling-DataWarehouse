import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging

from common.helpers import ConstantsProvider
from ingest.HR_System.full_load import HR_to_HDFS, HDFS_LandingZone_to_Hive_Staging

import yaml

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from abc import ABC, abstractmethod

# Factory to create an Airflow Task Operator Instance
class AirflowOperatorFactory:
    def create_airflow_operator(self, *args, **kwargs):
        pass


class PythonAirflowOperatorFactory(AirflowOperatorFactory):

    def create_airflow_operator(
        self,
        task_id: str,
        python_callable: callable,
        op_kwargs: dict,
        dag: DAG,
        *args,
        **kwargs,
    ):
        return PythonOperator(
            task_id=task_id,
            python_callable=python_callable,
            op_kwargs=op_kwargs,
            dag=dag,
        )


class BashAirflowOperatorFactory(AirflowOperatorFactory):

    def create_airflow_operator(
        self, task_id: str, bash_command: str, dag: DAG, *args, **kwargs
    ):
        return BashOperator(task_id=task_id, bash_command=bash_command, dag=dag)


class TaskGenerator(ABC):
    def __init__(self, dag: DAG, *args, **kwargs) -> None:
        self.dag = dag

    @abstractmethod
    def add_all_tasks(self, *args, **kwargs):
        """
        Creating a logical dictionary of tasks and metadata for creating airflow real tasks.
        """
        pass

    @abstractmethod
    def _add_tasks(self, *args, **kwargs):
        """
        Creating a dictionary of airflow tasks whose dependencies among them are specified.
        """
        pass

    @abstractmethod
    def _create_airflow_run_task(
        self, airflow_operator_factory: AirflowOperatorFactory, *args, **kwargs
    ):
        """
        Creating an atomic airflow task.
        """
        return airflow_operator_factory.create_airflow_operator(*args, **kwargs)

    @abstractmethod
    def _add_task_dependency(
        self,
        airflow_tasks: dict,
        *args,
        **kwargs,
    ):
        """
        Establishing the dependencies among airflow tasks.
        """
        pass


class HRFullLoadTaskGenerator(TaskGenerator):
    def __init__(self, dag: DAG, is_full_load: bool = True, *args, **kwargs) -> None:
        super().__init__(dag, *args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.source = ConstantsProvider.get_HR_source()

    def add_all_tasks(self, *args, **kwargs):
        self.logger.info(
            f"Parsing yaml file {ConstantsProvider.config_file_path(source=ConstantsProvider.get_HR_source())}..."
        )

        with open(
            ConstantsProvider.config_file_path(
                source=ConstantsProvider.get_HR_source()
            ),
            "r",
        ) as file:
            tables = yaml.load(file, yaml.Loader).get("full_load")

        self._add_tasks(tables=tables)

    def _add_tasks(self, tables: list, *args, **kwargs):
        airflow_tasks_map = {
            table: [
                self._create_airflow_run_task(
                    airflow_operator_factory=PythonAirflowOperatorFactory(),
                    task_id=f"ingest_{table}_from_{ConstantsProvider.get_HR_source()}",
                    python_callable=HR_to_HDFS,
                    op_kwargs={
                        "table": table,
                        "source": self.source,
                        "logger": self.logger,
                    },
                    dag=self.dag,
                ),
                self._create_airflow_run_task(
                    airflow_operator_factory=PythonAirflowOperatorFactory(),
                    task_id=f"ingest_{table}_from_HDFS_to_Hive",
                    python_callable=HDFS_LandingZone_to_Hive_Staging,
                    op_kwargs={
                        "table": table,
                        "source": self.source,
                        "logger": self.logger,
                    },
                    dag=self.dag,
                ),
            ]
            for table in tables
        }

        self._add_task_dependency(airflow_tasks=airflow_tasks_map)

    def _create_airflow_run_task(
        self, airflow_operator_factory: AirflowOperatorFactory, *args, **kwargs
    ):
        return super()._create_airflow_run_task(
            airflow_operator_factory, *args, **kwargs
        )

    def _add_task_dependency(self, airflow_tasks: dict, *args, **kwargs):
        self.logger.info("Building the dependency for airflow tasks ... ")

        for table in airflow_tasks:
            airflow_tasks[table][0] >> airflow_tasks[table][1]
