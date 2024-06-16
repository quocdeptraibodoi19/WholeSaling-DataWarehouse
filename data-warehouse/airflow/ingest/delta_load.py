import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import pandas as pd

import logging

from common.ingestion_strategies import (
    DataIngester,
    HiveStagingDeltaKeyIngestionStrategy,
    HDFSLandingZoneIngestionStrategy,
    HiveStagingIntestionStrategy,
)
from common.system_data_hooks import (
    get_system_datahook,
    HDFSDataHook,
    HiveDataHook,
)

from common.helpers import ConstantsProvider, DataManipulator, DataManipulatingManager

from datetime import datetime

from itertools import chain


def delta_source_to_HDFS(logger: logging.Logger, table_config: dict, source: str):
    table = table_config.get("table")

    source_sys = get_system_datahook(source=source)
    hive_sys = HiveDataHook()
    try:
        source_sys.connect()
        hive_sys.connect()

        LSET_query = f"""SELECT LSET FROM {ConstantsProvider.get_delta_key_table()} 
                WHERE source = '{source.lower()}' AND table = '{table.lower()}' AND is_log = '0'"""

        LSET_df = hive_sys.execute(query=LSET_query)
        LSET = str(LSET_df.to_dict("records")[0]["LSET"])
        logger.info(
            f"The latest LSET of the table {table} with the source {source} are: {LSET}"
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

        delta_load_sql = f"""SELECT {",".join(columns_selects)} FROM [{table}] WHERE CONVERT(DATETIME2, {custom_casts['ModifiedDate']}) > CONVERT(DATETIME2, '{LSET}', 126)"""

        logger.info(
            f"The query to retrieve the latest incremental data from table {table} and source {source} is: {delta_load_sql}"
        )

        data_collection = source_sys.execute(
            query=delta_load_sql, chunksize=ConstantsProvider.HR_query_chunksize()
        )

        data_manipulator = DataManipulator(
            data_collection=data_collection, logger=logger
        )

        data_collection = (
            data_manipulator.transform(
                DataManipulatingManager.standardlize_date_format(
                    column=ConstantsProvider.get_update_key(),
                    datetime_format="%Y-%m-%d %H:%M:%S.%f",
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

        logger.info(f"Moving data into HDFS...")
        hdfs_ingester = DataIngester(HDFSLandingZoneIngestionStrategy())
        hdfs_ingester.ingest(
            source_system=source,
            table_name=table,
            data_collection=data_collection,
            ingested_file_name=ConstantsProvider.get_deltaload_ingest_file(),
            is_full_load=False,
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        source_sys.disconnect()
        hive_sys.disconnect()


def delta_HDFS_LandingZone_to_Hive_Staging(
    logger: logging.Logger, table_config: dict, source: str
):
    table = table_config.get("table")

    hdfs_sys = HDFSDataHook()
    try:
        hdfs_sys.connect()

        table_schema = hdfs_sys.execute(command="data_schema")(
            table_name=table,
            source_name=source,
            file_name=ConstantsProvider.get_deltaload_ingest_file(),
            is_full_load=False,
        )

        logger.info(
            f"Creating the external table for incremental load ingested data from table {table} and source {source} with the columns {table_schema}"
        )

        hive_ingester = DataIngester(HiveStagingIntestionStrategy())
        hive_ingester.ingest(
            table_name=table,
            source_system=source,
            table_columns=table_schema,
            hive_table_name=ConstantsProvider.get_delta_table(source, table),
            is_full_load=False,
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hdfs_sys.disconnect()


def delete_source_to_HDFS(logger: logging.Logger, table_config: dict, source: str):
    table = table_config.get("table")
    delete_log_table = ConstantsProvider.delete_log_table_name(table)

    source_sys = get_system_datahook(source=source)
    hive_sys = HiveDataHook()
    try:
        source_sys.connect()
        hive_sys.connect()

        LSET_query = f"""SELECT LSET FROM {ConstantsProvider.get_delta_key_table()} 
                WHERE source = '{source.lower()}' AND table = '{table.lower()}' AND is_log = '1'"""
        LSET_df = hive_sys.execute(query=LSET_query)

        metadata_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{delete_log_table}'"

        logger.info(
            f"Getting columns of the table {delete_log_table} with query: {metadata_query}"
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

        logger.info(f"The columns of {delete_log_table} are: {columns_data}")

        custom_casts = table_config.get("custom_casts")
        custom_casts[ConstantsProvider.delete_date_custom_cast_delete_log()[0]] = (
            ConstantsProvider.delete_date_custom_cast_delete_log()[1]
        )

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

        if LSET_df.empty:
            delete_load_sql = (
                f"""SELECT {",".join(columns_selects)} FROM [{delete_log_table}]"""
            )
        else:
            LSET = str(LSET_df.to_dict("records")[0]["LSET"])
            if LSET == "None":
                delete_load_sql = (
                    f"""SELECT {",".join(columns_selects)} FROM [{delete_log_table}]"""
                )
            else:
                delete_load_sql = f"""SELECT {",".join(columns_selects)} FROM [{delete_log_table}] WHERE CONVERT(DATETIME2, {ConstantsProvider.delete_date_custom_cast_delete_log()[1]}) > CONVERT(DATETIME2, '{LSET}', 126)"""

        logger.info(
            f"The query to retrieve the latest incremental data from table {delete_log_table} and source {source} is: {delete_load_sql}"
        )

        data_collection = source_sys.execute(
            query=delete_load_sql, chunksize=ConstantsProvider.HR_query_chunksize()
        )

        data_manipulator = DataManipulator(
            data_collection=data_collection, logger=logger
        )

        data_collection = (
            data_manipulator.transform(
                DataManipulatingManager.standardlize_date_format(
                    column=ConstantsProvider.delete_date_custom_cast_delete_log()[0],
                    datetime_format="%Y-%m-%d %H:%M:%S.%f",
                )
            )
            .transform(
                DataManipulatingManager.mapping_column_data_collection(
                    column_1=ConstantsProvider.get_update_key(),
                    column_2=ConstantsProvider.delete_date_custom_cast_delete_log()[0],
                )
            )
            .transform(
                DataManipulatingManager.add_new_column_data_collection(
                    column=ConstantsProvider.soft_delete_meta_field(), val=True
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

        logger.info(f"Moving data into HDFS...")
        hdfs_ingester = DataIngester(HDFSLandingZoneIngestionStrategy())
        hdfs_ingester.ingest(
            source_system=source,
            table_name=table,
            data_collection=data_collection,
            ingested_file_name=ConstantsProvider.get_data_key_ingest_file(),
            HDFS_location_dir=ConstantsProvider.HDFS_LandingZone_delete_reconcile_base_dir(
                source, table, datetime.now().strftime("%Y-%m-%d")
            ),
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        source_sys.disconnect()


def delete_HDFS_LandingZone_to_Hive_Staging(
    logger: logging.Logger, table_config: dict, source: str
):
    table = table_config.get("table")

    hdfs_sys = HDFSDataHook()
    hive_sys = HiveDataHook()
    try:
        hdfs_sys.connect()
        hive_sys.connect()

        table_schema = hdfs_sys.execute(command="data_schema")(
            base_dir=ConstantsProvider.HDFS_LandingZone_delete_reconcile_base_dir(
                source, table, datetime.now().strftime("%Y-%m-%d")
            ),
            file_name=ConstantsProvider.get_data_key_ingest_file(),
        )

        logger.info(
            f"Creating the external table for delete load ingested data from table {table} and source {source} with the columns {table_schema}"
        )

        hive_ingester = DataIngester(HiveStagingIntestionStrategy())
        hive_ingester.ingest(
            table_name=table,
            source_system=source,
            table_columns=table_schema,
            hive_table_name=ConstantsProvider.get_reconcile_delete_table(source, table),
            HDFS_table_location_dir=(
                ConstantsProvider.HDFS_LandingZone_delete_reconcile_base_dir(
                    source, table, datetime.now().strftime("%Y-%m-%d")
                )
            ),
        )

        delete_date_field = ConstantsProvider.delete_date_custom_cast_delete_log()[0]
        LSET_load_hql = f"""SELECT MAX({delete_date_field}) AS {delete_date_field} FROM {ConstantsProvider.get_reconcile_delete_table(source, table)}"""
        logger.info(f"Getting the latest delete key with the hiveQL: {LSET_load_hql}")
        LSET_df = hive_sys.execute(query=LSET_load_hql)
        if not LSET_df.empty:
            LSET = str(LSET_df.to_dict("records")[-1][delete_date_field])
            delta_hive_ingester = DataIngester(HiveStagingDeltaKeyIngestionStrategy())
            delta_hive_ingester.ingest(
                source=source, table=table, LSET=LSET, is_log="1"
            )

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hdfs_sys.disconnect()


def reconciling_delta_delete_Hive_Staging(
    logger: logging.Logger, table_config: dict, source: str
):
    table = table_config.get("table")

    hive_sys = HiveDataHook()
    hdfs_sys = HDFSDataHook()
    try:
        hive_sys.connect()
        hdfs_sys.connect()

        delete_table_schema = hdfs_sys.execute(command="data_schema")(
            base_dir=ConstantsProvider.HDFS_LandingZone_delete_reconcile_base_dir(
                source, table, datetime.now().strftime("%Y-%m-%d")
            ),
            file_name=ConstantsProvider.get_data_key_ingest_file(),
        )
        filter_delete_table_schema = list(
            filter(
                lambda col: col
                != ConstantsProvider.delete_date_custom_cast_delete_log()[0],
                delete_table_schema,
            )
        )

        delta_upsert_hql = f"""INSERT INTO {ConstantsProvider.get_staging_table(source, table)}
                        SELECT * FROM {ConstantsProvider.get_delta_table(source, table)}"""

        delete_upsert_hql = f"""INSERT INTO {ConstantsProvider.get_staging_table(source, table)}
                        SELECT {", ".join(filter_delete_table_schema)} FROM {ConstantsProvider.get_reconcile_delete_table(source, table)}"""

        with hive_sys.connection.cursor() as cursor:
            cursor.execute(delete_upsert_hql)
            cursor.execute(delta_upsert_hql)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hive_sys.disconnect()
        hdfs_sys.disconnect()


def update_LSET(logger: logging.Logger, table_config: dict, source: str):
    table = table_config.get("table")

    hive_sys = HiveDataHook()
    hive_sys.connect()
    try:

        LSET_load_hql = f"""SELECT MAX(ModifiedDate) AS modifieddate FROM {ConstantsProvider.get_staging_table(source, table)}"""

        logger.info(f"Getting the latest delta key with the hiveQL: {LSET_load_hql}")

        LSET_df = hive_sys.execute(query=LSET_load_hql)

        LSET = str(LSET_df.to_dict("records")[-1]["modifieddate"])

        LSET_meta_table_ddl = f"""CREATE TABLE IF NOT EXISTS `{ConstantsProvider.get_delta_key_table()}` 
                                    ( 
                                        `source` STRING,
                                        `table` STRING,
                                        `is_log` STRING,
                                        `LSET` STRING,
                                        {ConstantsProvider.ingested_meta_field()} STRING
                                    )
                                    STORED AS PARQUET
                                """
        with hive_sys.connection.cursor() as cursor:
            logger.info(f"Creating Meta LSET table with ddl: {LSET_meta_table_ddl}")
            cursor.execute(LSET_meta_table_ddl)

        delta_hive_ingester = DataIngester(HiveStagingDeltaKeyIngestionStrategy())
        delta_hive_ingester.ingest(source=source, table=table, LSET=LSET)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hive_sys.disconnect()


def check_full_load_yet(logger: logging.Logger, table: str, source: str):
    hive_sys = HiveDataHook()

    try:
        hive_sys.connect()

        check_if_delta_key_exist_query = f"""SHOW TABLES IN {ConstantsProvider.get_staging_DW_name()} LIKE '{ConstantsProvider.get_delta_key_table()}'"""

        logger.info(
            f"Checking the existence of delta_keys table with: {check_if_delta_key_exist_query}"
        )

        checking_df = hive_sys.execute(query=check_if_delta_key_exist_query)

        if checking_df.empty:
            return False

        query = f"""SELECT LSET FROM {ConstantsProvider.get_delta_key_table()} 
                WHERE source = '{source.lower()}' AND table = '{table.lower()}' AND is_log = '0'"""

        logger.info(
            f"Check for table {table} from the source {source} has full loaded yet with query: {query}"
        )

        data_df = hive_sys.execute(query=query)

        if data_df.empty:
            return False
        
        LSET = str(data_df.to_dict("records")[0]["LSET"])
        if LSET == 'None':
            return False
        
        return True

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hive_sys.disconnect()
