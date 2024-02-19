import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging

from common.helpers import ConstantsProvider

import yaml

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
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


class PythonBranchAirflowOperatorFactory(AirflowOperatorFactory):
    def create_airflow_operator(
        self,
        task_id: str,
        python_callable: callable,
        dag: DAG,
        op_kwargs: dict = None,
        *args,
        **kwargs,
    ):
        return BranchPythonOperator(
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


class FullLoadTaskGenerator(TaskGenerator):
    def __init__(
        self, dag: DAG, source: str, airflow_task_funcs: dict, *args, **kwargs
    ) -> None:
        super().__init__(dag, *args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.source = source
        self.airflow_task_funcs = airflow_task_funcs

    def add_all_tasks(self, tables: list = None, *args, **kwargs):
        self.logger.info(
            f"Parsing yaml file {ConstantsProvider.config_file_path(source=self.source)}..."
        )

        if tables is None:
            with open(
                ConstantsProvider.config_file_path(source=self.source),
                "r",
            ) as file:
                tables = yaml.load(file, yaml.Loader).get("full_load")

        self._add_tasks(tables=tables)

    def _add_tasks(self, tables: list, *args, **kwargs):
        airflow_tasks_map = {
            table: [
                self._create_airflow_run_task(
                    airflow_operator_factory=PythonAirflowOperatorFactory(),
                    task_id=f"ingest_{table}_from_{self.source}",
                    python_callable=self.airflow_task_funcs.get("HR_to_HDFS"),
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
                    python_callable=self.airflow_task_funcs.get(
                        "HDFS_LandingZone_to_Hive_Staging"
                    ),
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


class DeltaLoadTaskGenerator(TaskGenerator):
    def __init__(
        self, dag: DAG, source: str, airflow_task_funcs: dict, *args, **kwargs
    ) -> None:
        super().__init__(dag, *args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.source = source
        self.airflow_task_funcs = airflow_task_funcs

    def add_all_tasks(self, tables_configs: list = None, *args, **kwargs):
        self.logger.info(
            f"Parsing yaml file {ConstantsProvider.config_file_path(source=self.source)}..."
        )

        if tables_configs is None:
            with open(
                ConstantsProvider.config_file_path(source=self.source),
                "r",
            ) as file:
                tables_configs = yaml.load(file, yaml.Loader).get("delta_load")

        self._add_tasks(tables_configs=tables_configs)

    def _add_tasks(self, tables_configs: list, *args, **kwargs):
        airflow_tasks_map = {
            table_config.get("table"): {
                "full_load": [
                    self._create_airflow_run_task(
                        airflow_operator_factory=PythonAirflowOperatorFactory(),
                        task_id=f"""ingest_{table_config.get("table")}_from_{self.source}""",
                        python_callable=self.airflow_task_funcs.get("HR_to_HDFS"),
                        op_kwargs={
                            "table": table_config.get("table"),
                            "source": self.source,
                            "logger": self.logger,
                        },
                        dag=self.dag,
                    ),
                    self._create_airflow_run_task(
                        airflow_operator_factory=PythonAirflowOperatorFactory(),
                        task_id=f"""ingest_{table_config.get("table")}_from_HDFS_to_Hive""",
                        python_callable=self.airflow_task_funcs.get(
                            "HDFS_LandingZone_to_Hive_Staging"
                        ),
                        op_kwargs={
                            "table": table_config.get("table"),
                            "source": self.source,
                            "logger": self.logger,
                        },
                        dag=self.dag,
                    ),
                    self._create_airflow_run_task(
                        airflow_operator_factory=PythonAirflowOperatorFactory(),
                        task_id=f"""update_detla_key_table_{self.source}_{table_config.get("table")}_Hive""",
                        python_callable=self.airflow_task_funcs.get(
                            "update_delta_keys"
                        ),
                        op_kwargs={
                            "table_config": table_config,
                            "source": self.source,
                            "logger": self.logger,
                        },
                        dag=self.dag,
                    ),
                ],
                "delta_load": [
                    self._create_airflow_run_task(
                        airflow_operator_factory=PythonAirflowOperatorFactory(),
                        task_id=f"""delete_detection_{table_config.get("table")}_{self.source}_HDFS""",
                        python_callable=self.airflow_task_funcs.get(
                            "delete_detection_HR_HDFS"
                        ),
                        op_kwargs={
                            "table_config": table_config,
                            "source": self.source,
                            "logger": self.logger,
                        },
                        dag=self.dag,
                    ),
                    self._create_airflow_run_task(
                        airflow_operator_factory=PythonAirflowOperatorFactory(),
                        task_id=f"""delete_detection_{table_config.get("table")}_HDFS_Hive""",
                        python_callable=self.airflow_task_funcs.get(
                            "delete_detection_HDFS_LandingZone_to_Hive_Staging"
                        ),
                        op_kwargs={
                            "table_config": table_config,
                            "source": self.source,
                            "logger": self.logger,
                        },
                        dag=self.dag,
                    ),
                    self._create_airflow_run_task(
                        airflow_operator_factory=PythonAirflowOperatorFactory(),
                        task_id=f"""delta_load_{table_config.get("table")}_{self.source}_HDFS""",
                        python_callable=self.airflow_task_funcs.get("delta_HR_to_HDFS"),
                        op_kwargs={
                            "table_config": table_config,
                            "source": self.source,
                            "logger": self.logger,
                        },
                        dag=self.dag,
                    ),
                    self._create_airflow_run_task(
                        airflow_operator_factory=PythonAirflowOperatorFactory(),
                        task_id=f"""delta_load_{table_config.get("table")}_HDFS_Hive""",
                        python_callable=self.airflow_task_funcs.get(
                            "delta_HDFS_LandingZone_to_Hive_Staging"
                        ),
                        op_kwargs={
                            "table_config": table_config,
                            "source": self.source,
                            "logger": self.logger,
                        },
                        dag=self.dag,
                    ),
                    self._create_airflow_run_task(
                        airflow_operator_factory=PythonAirflowOperatorFactory(),
                        task_id=f"""delta_load_{table_config.get("table")}_Reconcile_Hive""",
                        python_callable=self.airflow_task_funcs.get(
                            "reconciling_delta_delete_Hive_Staging"
                        ),
                        op_kwargs={
                            "table_config": table_config,
                            "source": self.source,
                            "logger": self.logger,
                        },
                        dag=self.dag,
                    ),
                ],
                "branch_detector": self._create_airflow_run_task(
                    airflow_operator_factory=PythonBranchAirflowOperatorFactory(),
                    task_id=f"""check_for_{table_config.get("table")}_FullLoad_yet""",
                    python_callable=self._branch_detector_func,
                    op_kwargs={
                        "table": table_config.get("table"),
                        "true_task": f"""delete_detection_{table_config.get("table")}_{self.source}_HDFS""",
                        "false_task": f"""ingest_{table_config.get("table")}_from_{self.source}""",
                    },
                    dag=self.dag,
                ),
            }
            for table_config in tables_configs
        }

        self._add_task_dependency(airflow_tasks=airflow_tasks_map)

    def _branch_detector_func(self, table: str, true_task: str, false_task: str):
        self.logger.info(
            f"Detecting what is the next task of the table {table} to be executed: {true_task} or {false_task} ... "
        )

        if self.airflow_task_funcs.get("check_full_load_yet")(
            logger=self.logger,
            source=self.source,
            table=table,
        ):
            task_to_implemented = true_task
        else:
            task_to_implemented = false_task

        self.logger.info(f"The task to be excuted is {task_to_implemented}")

        return task_to_implemented

    def _create_airflow_run_task(
        self, airflow_operator_factory: AirflowOperatorFactory, *args, **kwargs
    ):
        return super()._create_airflow_run_task(
            airflow_operator_factory, *args, **kwargs
        )

    def _add_task_dependency(self, airflow_tasks: dict, *args, **kwargs):
        self.logger.info("Building the dependency for airflow tasks ... ")

        for table in airflow_tasks:
            airflow_tasks[table]["full_load"][0] >> airflow_tasks[table]["full_load"][1]
            airflow_tasks[table]["full_load"][1] >> airflow_tasks[table]["full_load"][2]
            (
                airflow_tasks[table]["delta_load"][0]
                >> airflow_tasks[table]["delta_load"][1]
            )
            (
                airflow_tasks[table]["delta_load"][1]
                >> airflow_tasks[table]["delta_load"][2]
            )
            (
                airflow_tasks[table]["delta_load"][2]
                >> airflow_tasks[table]["delta_load"][3]
            )
            (
                airflow_tasks[table]["delta_load"][3]
                >> airflow_tasks[table]["delta_load"][4]
            )
            airflow_tasks[table]["branch_detector"] >> [
                airflow_tasks[table]["full_load"][0],
                airflow_tasks[table]["delta_load"][0],
            ]
