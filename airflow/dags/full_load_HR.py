import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from datetime import datetime, timedelta
from airflow.models.dag import DAG

from common.helpers import ConstantsProvider

with DAG(
    "full_load_HR",
    default_args=ConstantsProvider.default_dag_args(),
    description="This is the first DAG to be executed",
    schedule=timedelta(hours=3),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    pass
