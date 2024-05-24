import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging

logger = logging.getLogger(__name__)

from datetime import datetime, timedelta

from common.helpers import ConstantsProvider

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import json

from ingest.data_firewall import (
    compare_and_identify_ambiguous_records,
    move_ambiguous_records_to_DQ_table,
    convert_json_to_dfs,
)

def ambiguous_recods_resolving(ambiguous_data_json_records: str, logger: logging.Logger):
    ambiguous_data_collection = convert_json_to_dfs(ambiguous_data_json_records)
    move_ambiguous_records_to_DQ_table(logger, ambiguous_data_collection)

json_ambiguous_records = compare_and_identify_ambiguous_records(
    hive_tables=["product_management_platform_product"],
    primary_key="ProductID",
    ambiguous_key="Name",
    logger=logger
)

json_ambiguous_records_str = json.dumps(json_ambiguous_records)

with DAG(
    "Data_Firewall",
    default_args=ConstantsProvider.default_dag_args(),
    description=f"A data pipleline for firewall check",
    schedule=timedelta(hours=24),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "ambiguous_data_json_records": json_ambiguous_records_str
    }
) as dag:

    ambiguous_recods_identification = PythonOperator(
        task_id="Ambiguous_Records_Data_Firewall",
        python_callable=ambiguous_recods_resolving,
        op_kwargs={
            "ambiguous_data_json_records": "{{ params.ambiguous_data_json_records }}",
            "logger": logger,
        },
        dag=dag,
    )
