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
from common.helpers import ConstantsProvider, DataManipulator, DataManipulatingManager

import pandas as pd


def HR_to_HDFS(logger: logging.Logger, table: str, source: str):
    hr_sys = HRSystemDataHook()
    hr_sys.connect()
    try:
        metadata_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'"
        logger.info(
            f"Getting columns of the table {table} with query: {metadata_query}"
        )
        columns_data = list(
            map(
                lambda data: data[0],
                hr_sys.execute(query=metadata_query).itertuples(index=False, name=None),
            )
        )

        logger.info(f"The columns of {table} are: {columns_data}")

        query_columns = [
            "CONVERT(NVARCHAR(MAX), " + "[" + col + "]" + ") AS " + "[" + col + "]"
            for col in columns_data
        ]
        
        query = "SELECT " + ",".join(query_columns) + f" FROM {table}"
        logger.info(f"Getting data from {table} in {source} with query: {query}")
        data_collection = hr_sys.execute(
            query=query, chunksize=ConstantsProvider.HR_query_chunksize()
        )

        data_manipulator = DataManipulator(
            data_collection=data_collection, logger=logger
        )

        for date_field in ConstantsProvider.get_HR_date_fields_for_standardization():
            if date_field in columns_data:
                data_manipulator.transform(
                    DataManipulatingManager.standardlize_date_format(
                        column=date_field,
                        datetime_format=ConstantsProvider.get_sources_datetime_format_standardization(),
                    )
                )

        data_collection = (
            data_manipulator.transform(
                DataManipulatingManager.add_new_column_data_collection(
                    column=ConstantsProvider.ingested_meta_field(),
                    val=pd.to_datetime(datetime.now().strftime("%Y-%m-%d")),
                )
            )
            .transform(
                DataManipulatingManager.add_new_column_data_collection(
                    column=ConstantsProvider.soft_delete_meta_field(), val=False
                )
            )
            .execute()
        )

        logger.info(f"Moving data into HDFS...")
        hdfs_ingester = DataIngester(HDFSLandingZoneIngestionStrategy())
        hdfs_ingester.ingest(
            source_system=source,
            table_name=table,
            data_collection=data_collection,
            ingested_file_name=ConstantsProvider.get_fullload_ingest_file(),
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hr_sys.disconnect()


def HDFS_LandingZone_to_Hive_Staging(logger: logging.Logger, table: str, source: str):
    hdfs_sys = HDFSDataHook()
    try:
        hdfs_sys.connect()

        table_schema = hdfs_sys.execute(
            command="data_schema",
            table_name=table,
            source_name=source,
            file_name=ConstantsProvider.get_fullload_ingest_file(),
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
            hive_table_name=ConstantsProvider.get_staging_table(source, table),
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hdfs_sys.disconnect()
