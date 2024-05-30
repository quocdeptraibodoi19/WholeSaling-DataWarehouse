import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging

from datetime import datetime

from common.system_data_hooks import (
    get_system_datahook,
    HDFSDataHook,
)
from common.ingestion_strategies import (
    HDFSLandingZoneIngestionStrategy,
    HiveStagingIntestionStrategy,
    DataIngester,
)
from common.helpers import ConstantsProvider, DataManipulator, DataManipulatingManager

import pandas as pd

from itertools import chain


def full_source_to_HDFS(logger: logging.Logger, table_config: dict, source: str, is_delta_used: bool = False):
    table = table_config.get("table")
    custom_full_load_sql = table_config.get("custom_full_load_sql")

    source_sys = get_system_datahook(source=source)
    try:
        source_sys.connect()

        if custom_full_load_sql is None:
            logger.info(
                f"Custom load SQL is not specified ... Gonna construct the load SQL for table {table} from source {source}"
            )

            metadata_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'"

            logger.info(
                f"Getting columns of the table {table} with query: {metadata_query}"
            )
            columns_data = list(
                chain.from_iterable(
                    map(
                        lambda data_col: list(map(lambda data: data[0], data_col)),
                        map(
                            lambda data: data.itertuples(index=False, name=None),
                            source_sys.execute(
                                query=metadata_query,
                                chunksize=ConstantsProvider.HR_query_chunksize(),
                            ),
                        ),
                    )
                )
            )

            logger.info(f"The columns of {table} are: {columns_data}")

            custom_casts = table_config.get("custom_casts")
            custom_casts_fields = (
                list(custom_casts.keys()) if custom_casts is not None else []
            )

            columns_selects = map(
                lambda col: (
                    custom_casts[col] + " AS " + "[" + col + "]"
                    if col in custom_casts_fields
                    else f"CONVERT(NVARCHAR(MAX), [{col}]) AS [{col}]"
                ),
                columns_data,
            )

            query = f"""SELECT {",".join(columns_selects)} FROM [{table}]"""
        else:
            query = custom_full_load_sql

        logger.info(f"Getting data from {table} in {source} with query: {query}")

        data_collection = source_sys.execute(
            query=query, chunksize=ConstantsProvider.HR_query_chunksize()
        )

        data_manipulator = DataManipulator(
            data_collection=data_collection, logger=logger
        )

        if custom_casts is not None and custom_casts.get('ModifiedDate') is not None:
            if not is_delta_used:
                data_collection = (
                    data_manipulator.transform(
                        DataManipulatingManager.standardlize_date_format(
                            column=ConstantsProvider.get_update_key(),
                            datetime_format="%Y-%m-%d %H:%M:%S.%f"
                        )
                    )
                    .transform(
                        DataManipulatingManager.add_new_column_data_collection(
                            column=ConstantsProvider.ingested_meta_field(),
                            val=pd.to_datetime(datetime.now().strftime("%Y-%m-%d")),
                        )
                    )
                    .execute()
                )
            else:
                data_collection = (
                    data_manipulator.transform(
                        DataManipulatingManager.standardlize_date_format(
                            column=ConstantsProvider.get_update_key(),
                            datetime_format="%Y-%m-%d %H:%M:%S.%f"
                        )
                    )
                    .transform(
                        DataManipulatingManager.add_new_column_data_collection(
                            column=ConstantsProvider.soft_delete_meta_field(), val=False
                        )
                    )
                    .transform(
                        DataManipulatingManager.add_new_column_data_collection(
                            column=ConstantsProvider.ingested_meta_field(),
                            val=pd.to_datetime(datetime.now().strftime("%Y-%m-%d")),
                        )
                    )
                    .execute()
                )
        else:
            data_collection = (
                data_manipulator.transform(
                    DataManipulatingManager.add_new_column_data_collection(
                        column=ConstantsProvider.ingested_meta_field(),
                        val=pd.to_datetime(datetime.now().strftime("%Y-%m-%d")),
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
        source_sys.disconnect()


def HDFS_LandingZone_to_Hive_Staging(logger: logging.Logger, table: str, source: str):
    hdfs_sys = HDFSDataHook()
    try:
        table_schema = hdfs_sys.execute(command="data_schema")(
            table_name=table,
            source_name=source,
            file_name=ConstantsProvider.get_fullload_ingest_file(),
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
