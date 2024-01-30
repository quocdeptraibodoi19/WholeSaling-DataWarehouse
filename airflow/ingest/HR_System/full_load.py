import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging

logger = logging.getLogger(__name__)

from datetime import datetime

from common.ingester import (
    HRSystemDataHook,
    HDFSLandingZoneDataHook,
    PrestoHiveStagingDataHook,
    HiveStagingDataHook,
)
from common.helpers import ConstantsProvider


def HR_to_HDFS(table: str, source: str):
    hr_sys = HRSystemDataHook()
    hr_sys.connect()
    try:
        metadata_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'"
        logger.info(
            f"Getting columns of the table {table} with query: {metadata_query}"
        )
        columns_data = hr_sys.receive_data(query=metadata_query)

        query_columns = list(
            map(
                lambda data: "CONVERT(NVARCHAR(MAX), " + data[0] + ") AS " + data[0],
                columns_data.itertuples(index=False, name=None),
            )
        )
        query = "SELECT " + ",".join(query_columns) + f" FROM {table}"
        logger.info(f"Getting data from {table} in {source} with query: {query}")
        data_collection = hr_sys.receive_data(
            query=query, chunksize=ConstantsProvider.HR_query_chunksize()
        )
        
        logger.info(f"Moving data into HDFS...")
        hdfs_sys = HDFSLandingZoneDataHook()
        hdfs_sys.connect()
        try:
            hdfs_sys.move_data(
                source_system=source,
                table_name=table,
                data_collection=data_collection,
            )
        finally:
            hdfs_sys.disconnect()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hr_sys.disconnect()


def HDFS_LandingZone_to_Hive_Staging(table: str, source: str):
    hive_sys = HiveStagingDataHook()
    hive_sys.connect()
    try:
        hdfs_sys = HDFSLandingZoneDataHook()
        hdfs_sys.connect()
        try:
            table_schema = hdfs_sys.get_data_schema(
                table, source, datetime.now().strftime("%Y-%m-%d")
            )

            logger.info(f"Creating the external table for ingested data from table {table} and source {source} with the columns {table_schema}")
            
            hive_sys.move_data(table, source, table_schema)
        finally:
            hdfs_sys.disconnect()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hive_sys.disconnect()
