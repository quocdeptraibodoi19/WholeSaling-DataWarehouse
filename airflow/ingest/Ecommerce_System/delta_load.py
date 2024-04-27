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
    EcommerceSystemDataHook,
    HDFSDataHook,
    HiveDataHook,
)

from common.helpers import ConstantsProvider, DataManipulator, DataManipulatingManager

from datetime import datetime

from itertools import chain


def delta_HR_to_HDFS(logger: logging.Logger, table_config: dict, source: str):
    table = table_config.get("table")

    ecom_sys = EcommerceSystemDataHook()
    hive_sys = HiveDataHook()
    try:
        ecom_sys.connect()
        hive_sys.connect()

        LSET_query = f"""SELECT LSET FROM {ConstantsProvider.get_delta_key_table()} 
                WHERE schema = '{ConstantsProvider.get_staging_DW_name()}' AND table = '{ConstantsProvider.get_staging_table(source, table)}'"""

        LSET_df = hive_sys.execute(query=LSET_query)
        LSET = str(LSET_df.to_dict("records")[0]["LSET"])
        logger.info(
            f"The latest LSET of the table {table} with the source {source} are: {LSET}"
        )

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
                        ecom_sys.execute(
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

        delta_load_sql = f"""SELECT {",".join(columns_selects)} FROM [{table}] WHERE (DATEPART(MILLISECOND, [ModifiedDate])) > (DATEPART(MILLISECOND, CONVERT(DATETIME2, '{LSET}', 126)))"""

        logger.info(
            f"The query to retrieve the latest incremental data from table {table} and source {source} is: {delta_load_sql}"
        )

        data_collection = ecom_sys.execute(
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
            is_full_load=False
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        ecom_sys.disconnect()
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
            is_full_load=False
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
            is_full_load=False
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hdfs_sys.disconnect()


def delete_detection_HR_HDFS(logger: logging.Logger, table_config: dict, source: str):
    table = table_config.get("table")
    primary_keys = table_config.get("primary_keys")

    ecom_sys = EcommerceSystemDataHook()
    try:
        ecom_sys.connect()

        existed_data_query = f"""SELECT {",".join(primary_keys)} FROM [{table}]"""

        data_collection = ecom_sys.execute(
            query=existed_data_query,
            chunksize=ConstantsProvider.HR_query_chunksize(),
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
        ecom_sys.disconnect()


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
            is_full_load=False
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

        reconcile_DDL_hql = f"""CREATE TABLE IF NOT EXISTS {ConstantsProvider.get_delta_reconcile_delete_temp_view_table(source, table)} AS 
                SELECT {",".join(map(lambda col: "t4." + "`" + col + "`", table_schema))} FROM 
                (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {",".join(map(lambda key: f"`{key}`", primary_keys))} ORDER BY ModifiedDate DESC) rn
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
            f"reconcile_DDL_hql : {reconcile_DDL_hql}"
        )
        logger.info(
            f"Creating the view to reconcile the data with query: {reconcile_DDL_hql}"
        )

        reconcile_hql = f"""INSERT OVERWRITE TABLE {ConstantsProvider.get_staging_table(source, table)} 
                    SELECT * FROM {ConstantsProvider.get_delta_reconcile_delete_temp_view_table(source, table)}"""

        drop_reconcile_DDL_hql = f"DROP TABLE {ConstantsProvider.get_delta_reconcile_delete_temp_view_table(source, table)}"
        drop_delta_table_DDL_hql = f"DROP TABLE {ConstantsProvider.get_delta_table(source, table)}"

        with hive_sys.connection.cursor() as cursor:
            cursor.execute(reconcile_DDL_hql)
            cursor.execute(reconcile_hql)
            cursor.execute(drop_reconcile_DDL_hql)
            cursor.execute(drop_delta_table_DDL_hql)

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

        logger.info(
            f"Custom delta key load HQL is not specified ... Gonna construct the delta key load HQL for table {table} from source {source}"
        )

        LSET_load_hql = f"""SELECT MAX(ModifiedDate) AS modifieddate FROM {ConstantsProvider.get_staging_table(source, table)}"""

        logger.info(f"Getting the latest delta key with the hiveQL: {LSET_load_hql}")

        LSET_df = hive_sys.execute(query=LSET_load_hql)

        LSET = str(LSET_df.to_dict("records")[-1]['modifieddate'])
        logger.info(
            f"""The latest LSET from table '{ConstantsProvider.get_staging_table(source, table)}' and schema '{ConstantsProvider.get_staging_DW_name()}': {LSET}"""
        )

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
                WHERE schema = '{ConstantsProvider.get_staging_DW_name()}' AND table = '{ConstantsProvider.get_staging_table(source, table)}'"""

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
