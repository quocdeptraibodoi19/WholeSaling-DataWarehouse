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
    WholeSaleSystemDataHook,
    HDFSDataHook,
    HiveDataHook,
)

from common.helpers import ConstantsProvider, DataManipulator, DataManipulatingManager

from datetime import datetime

import ast

from itertools import chain


def delta_HR_to_HDFS(logger: logging.Logger, table_config: dict, source: str):
    table = table_config.get("table")
    custom_delta_load_sql = table_config.get("custom_delta_load_sql")
    delta_keys = table_config.get("delta_keys")

    wholesale_sys = WholeSaleSystemDataHook()
    hive_sys = HiveDataHook()

    try:
        wholesale_sys.connect()
        hive_sys.connect()

        delta_keys_query = f"""SELECT delta_keys FROM {ConstantsProvider.get_delta_key_table()} 
                WHERE "schema" = '{ConstantsProvider.get_staging_DW_name()}' AND "table" = '{ConstantsProvider.get_staging_table(source, table)}'"""

        delta_keys_df = hive_sys.execute(query=delta_keys_query)
        delta_keys = ast.literal_eval(delta_keys_df.to_dict("records")[0]["delta_keys"])
        logger.info(
            f"The latest delta keys of the table {table} with the source {source} are: {delta_keys}"
        )

        if custom_delta_load_sql is None:
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
                            wholesale_sys.execute(
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

            delta_conditions = map(
                lambda key: f"([{key}] > " + "'{" + f"{key.lower()}" + "}')", delta_keys
            )

            delta_load_sql = f"""SELECT {",".join(columns_selects)} FROM [{table}] WHERE {" AND ".join(delta_conditions)}"""
        else:
            delta_load_sql = custom_delta_load_sql

        delta_load_sql = delta_load_sql.format(table=table, **delta_keys)
        logger.info(
            f"The query to retrieve the latest incremental data from table {table} and source {source} is: {delta_load_sql}"
        )

        data_collection = wholesale_sys.execute(
            query=delta_load_sql, chunksize=ConstantsProvider.HR_query_chunksize()
        )

        data_manipulator = DataManipulator(
            data_collection=data_collection, logger=logger
        )

        data_collection = (
            data_manipulator.transform(
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
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        wholesale_sys.disconnect()
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
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hdfs_sys.disconnect()


def delete_detection_HR_HDFS(logger: logging.Logger, table_config: dict, source: str):
    table = table_config.get("table")
    primary_keys = table_config.get("primary_keys")

    hive_sys = HiveDataHook()
    try:
        hive_sys.connect()

        existed_data_query = f"""SELECT {",".join(primary_keys)} 
            FROM {ConstantsProvider.get_staging_table(source=source, table=table)} 
            WHERE {ConstantsProvider.soft_delete_meta_field()} = 'False'"""

        data_collection = hive_sys.execute(
            query=existed_data_query,
            chunksize=ConstantsProvider.Presto_query_chunksize(),
        )

        logger.info(
            f"Detecting the current existed data in DataWarehouse with {existed_data_query} ... with data: {data_collection}"
        )

        data_manipulator = DataManipulator(
            data_collection=data_collection, logger=logger
        )

        data_collection = data_manipulator.transform(
            DataManipulatingManager.add_new_column_data_collection(
                column=ConstantsProvider.ingested_meta_field(),
                val=pd.to_datetime(datetime.now().strftime("%Y-%m-%d")),
            )
        ).execute()

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
        hive_sys.disconnect()


def delete_detection_HDFS_LandingZone_to_Hive_Staging(
    logger: logging.Logger, table_config: dict, source: str
):
    table = table_config.get("table")
    primary_keys = table_config.get("primary_keys")

    hdfs_sys = HDFSDataHook()
    try:
        hdfs_sys.connect()

        logger.info(
            f"Creating the external table for data key from table {table} and source {source} with the columns {primary_keys}"
        )

        hive_ingester = DataIngester(HiveStagingIntestionStrategy())
        hive_ingester.ingest(
            table_name=table,
            source_system=source,
            table_columns=primary_keys,
            hive_table_name=ConstantsProvider.get_reconcile_delete_table(source, table),
            HDFS_table_location_dir=(
                ConstantsProvider.HDFS_LandingZone_delete_reconcile_base_dir(
                    source, table, datetime.now().strftime("%Y-%m-%d")
                )
            ),
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
    delta_keys = table_config.get("delta_keys")
    primary_keys = table_config.get("primary_keys")

    hive_sys = HiveDataHook()
    hdfs_sys = HDFSDataHook()
    try:
        hive_sys.connect()
        hdfs_sys.connect()

        # TODO: Instead of gettint data schema from HDFS like this, getting from Hive DW because staging is insecure.
        table_schema = hdfs_sys.execute(command="data_schema")(
            table_name=table,
            source_name=source,
            file_name=ConstantsProvider.get_deltaload_ingest_file(),
        )

        selected_cols = map(
            lambda col: (
                f"t1.`{col}`"
                if col != ConstantsProvider.soft_delete_meta_field()
                else (
                    "'TRUE' AS " + ConstantsProvider.soft_delete_meta_field()
                    if col != ConstantsProvider.ingested_meta_field()
                    else datetime.now().strftime("%Y-%m-%d")
                    + " AS "
                    + ConstantsProvider.ingested_meta_field()
                )
            ),
            table_schema,
        )
        delete_detection_hql = f"""SELECT {",".join(selected_cols)} 
                    FROM {ConstantsProvider.get_staging_table(source, table)} t1
                    WHERE NOT EXISTS (
                        SELECT * FROM {ConstantsProvider.get_reconcile_delete_table(source, table)} t2
                        WHERE {" AND ".join(map(lambda key: f"t1.`{key}` = t2.`{key}`", primary_keys))}
                    )
                """

        reconcile_DDL_hql = f"""CREATE VIEW IF NOT EXISTS {ConstantsProvider.get_delta_reconcile_delete_temp_view_table(source, table)} AS 
                SELECT {",".join(map(lambda col: "t4." + "`" + col + "`", table_schema))} FROM 
                (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {",".join(map(lambda key: f"`{key}`", primary_keys))} ORDER BY {",".join(map(lambda key: f"`{key}`", delta_keys))} DESC) rn
                    FROM (
                        SELECT * FROM {ConstantsProvider.get_staging_table(source, table)}
                        UNION ALL
                        SELECT * FROM {ConstantsProvider.get_delta_table(source, table)}
                        UNION ALL
                        SELECT * FROM ( {delete_detection_hql} ) subquery
                    ) t3
                ) t4 WHERE t4.rn = 1
            """

        logger.info(
            f"Creating the view to reconcile the data with query: {reconcile_DDL_hql}"
        )

        reconcile_hql = f"""INSERT OVERWRITE TABLE {ConstantsProvider.get_staging_table(source, table)} 
                    SELECT * FROM {ConstantsProvider.get_delta_reconcile_delete_temp_view_table(source, table)}"""

        drop_reconcile_DDL_hql = f"DROP VIEW {ConstantsProvider.get_delta_reconcile_delete_temp_view_table(source, table)}"

        with hive_sys.connection.cursor() as cursor:
            cursor.execute(reconcile_DDL_hql)
            cursor.execute(reconcile_hql)
            cursor.execute(drop_reconcile_DDL_hql)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hive_sys.disconnect()
        hdfs_sys.disconnect()


def update_delta_keys(logger: logging.Logger, table_config: dict, source: str):
    table = table_config.get("table")
    custom_deltakey_load_hql = table_config.get("custom_deltakey_load_hql")
    delta_keys = table_config.get("delta_keys")

    hive_sys = HiveDataHook()
    hive_sys.connect()
    try:
        if custom_deltakey_load_hql is None:
            logger.info(
                f"Custom delta key load HQL is not specified ... Gonna construct the delta key load HQL for table {table} from source {source}"
            )

            delta_keys_selects = map(
                lambda delta_key: f"MAX({delta_key}) AS {delta_key.lower()}", delta_keys
            )
            delta_key_load_hql = f"""SELECT {",".join(delta_keys_selects)} FROM {ConstantsProvider.get_staging_table(source, table)}"""
        else:
            delta_key_load_hql = custom_deltakey_load_hql

        logger.info(
            f"Getting the latest delta key with the hiveQL: {delta_key_load_hql}"
        )

        delta_keys_df = hive_sys.execute(query=delta_key_load_hql)

        delta_keys_dict = delta_keys_df.to_dict("records")[-1]
        logger.info(
            f"""The latest delta key from table '{ConstantsProvider.get_staging_table(source, table)}' and schema '{ConstantsProvider.get_staging_DW_name()}': {str(delta_keys_dict)}"""
        )

        delta_keys_dict = {key: str(val) for key, val in delta_keys_dict.items()}
        logger.info(
            f"Convert all value of keys in the dictionary to string: {delta_keys_dict}"
        )

        delta_hive_ingester = DataIngester(HiveStagingDeltaKeyIngestionStrategy())
        delta_hive_ingester.ingest(
            source=source, table=table, delta_keys_dict=delta_keys_dict
        )

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hive_sys.disconnect()


def check_full_load_yet(logger: logging.Logger, table: str, source: str):
    hive_sys = HiveDataHook()

    try:
        hive_sys.connect()

        check_if_delta_key_exist_query = f"""SELECT * FROM information_schema.tables WHERE table_schema = '{ConstantsProvider.get_staging_DW_name()}' AND table_name = '{ConstantsProvider.get_delta_key_table()}'"""

        logger.info(
            f"Checking the existence of delta_keys table with: {check_if_delta_key_exist_query}"
        )

        checking_df = hive_sys.execute(query=check_if_delta_key_exist_query)

        if checking_df.empty:
            return False

        query = f"""SELECT delta_keys FROM {ConstantsProvider.get_delta_key_table()} 
                WHERE "schema" = '{ConstantsProvider.get_staging_DW_name()}' AND "table" = '{ConstantsProvider.get_staging_table(source, table)}'"""

        logger.info(
            f"Check for table {table} from the source {source} has full loaded yet with query: {query}"
        )

        data_df = hive_sys.execute(query=query)

        return data_df.empty is False

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hive_sys.disconnect()
