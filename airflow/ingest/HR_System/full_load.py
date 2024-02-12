import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging

from datetime import datetime

from common.system_data_hooks import (
    HRSystemDataHook,
    HDFSDataHook,
)
from common.ingestion_strategies import (
    HDFSLandingZoneIngestionStrategy,
    HiveStagingIntestionStrategy,
    DataIngester,
)
from common.helpers import ConstantsProvider


def HR_to_HDFS(logger: logging.Logger, table: str, source: str):
    hr_sys = HRSystemDataHook()
    hr_sys.connect()
    try:
        metadata_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'"
        logger.info(
            f"Getting columns of the table {table} with query: {metadata_query}"
        )
        columns_data = hr_sys.execute(query=metadata_query)

        query_columns = list(
            map(
                lambda data: "CONVERT(NVARCHAR(MAX), "
                + "["
                + data[0]
                + "]"
                + ") AS "
                + "["
                + data[0]
                + "]",
                columns_data.itertuples(index=False, name=None),
            )
        )
        query = "SELECT " + ",".join(query_columns) + f" FROM {table}"
        logger.info(f"Getting data from {table} in {source} with query: {query}")
        data_collection = hr_sys.execute(
            query=query, chunksize=ConstantsProvider.HR_query_chunksize()
        )

        logger.info(
            f"Standarlizing data (column modifieddate) with format '%b %d %Y %I:%M%p' ..."
        )
        changed_data_collection = ConstantsProvider.standardlize_date_format(
            data_collection=data_collection,
            column="ModifiedDate",
            datetime_format="%b %d %Y %I:%M%p",
            logger=logger,
        )

        logger.info(f"Moving data into HDFS...")
        hdfs_ingester = DataIngester(HDFSLandingZoneIngestionStrategy())
        hdfs_ingester.ingest(
            source_system=source,
            table_name=table,
            data_collection=changed_data_collection,
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hr_sys.disconnect()


def HDFS_LandingZone_to_Hive_Staging(logger: logging.Logger, table: str, source: str):
    hdfs_sys = HDFSDataHook()
    hdfs_sys.connect()
    try:
        table_schema = hdfs_sys.execute(
            command="data_schema",
            table_name=table,
            source_name=source,
            date_str=datetime.now().strftime("%Y-%m-%d"),
        )

        logger.info(
            f"Creating the external table for ingested data from table {table} and source {source} with the columns {table_schema}"
        )

        hive_ingester = DataIngester(HiveStagingIntestionStrategy())
        hive_ingester.ingest(
            table_name=table,
            source_system=source,
            table_columns=table_schema,
            is_full_load=True,
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hdfs_sys.disconnect()
