import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import pandas as pd

import logging

from common.ingester import DeltaKeyHiveStagingDataHook
from common.helpers import ConstantsProvider

from datetime import datetime


def delta_HR_to_HDFS(logger: logging.Logger, table_config: dict, source: str):
    pass


def delta_HDFS_LandingZone_to_Hive_Staging(
    logger: logging.Logger, table_config: dict, source: str
):
    pass


def reconcile_HR_to_HDFS(logger: logging.Logger, table_config: dict, source: str):
    pass


def reconcile_HDFS_LandingZone_to_Hive_Staging(
    logger: logging.Logger, table_config: dict, source: str
):
    pass


def recocile_to_Hive_Staging(logger: logging.Logger, table_config: dict, source: str):
    pass


def delta_to_Hive_Staging(logger: logging.Logger, table_config: dict, source: str):
    pass


def update_delta_keys(logger: logging.Logger, table_config: dict, source: str):
    table = table_config.get("table")
    delta_load_hql = table_config.get("delta_load_hql")

    delta_load_hql = delta_load_hql.format(
        hive_table=ConstantsProvider.get_staging_table(source, table)
    )
    logger.info(f"Getting the latest delta key with the hiveQL: {delta_load_hql}")

    delta_key_hive_sys = DeltaKeyHiveStagingDataHook()
    delta_key_hive_sys.connect()
    try:
        delta_keys_df = delta_key_hive_sys.receive_data(query=delta_load_hql)

        delta_keys_dict = delta_keys_df.to_dict("records")[-1]
        logger.info(
            f"""The latest delta key from table '{ConstantsProvider.get_staging_table(source, table)}' and schema 'staging': {str(delta_keys_dict)}"""
        )

        delta_keys_dict = {key: str(val) for key, val in delta_keys_dict.items()}
        logger.info(
            f"Convert all value of keys in the dictionary to string: {delta_keys_dict}"
        )

        delta_key_hive_sys.move_data(
            source=source, table=table, delta_keys_dict=delta_keys_dict
        )

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        delta_key_hive_sys.disconnect()
