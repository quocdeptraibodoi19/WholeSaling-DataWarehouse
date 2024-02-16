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
    PrestoDataHook,
    HRSystemDataHook,
    HDFSDataHook,
    HiveDataHook,
)

from common.helpers import ConstantsProvider, DataManipulator, DataManipulatingManager

from datetime import datetime

import ast


def delta_HR_to_HDFS(logger: logging.Logger, table_config: dict, source: str):
    table = table_config.get("table")
    delta_load_sql = table_config.get("custom_load_sql")

    hr_sys = HRSystemDataHook()
    presto_sys = PrestoDataHook()

    try:
        hr_sys.connect()
        presto_sys.connect()
        
        delta_keys_query = f"SELECT delta_keys FROM {ConstantsProvider.get_delta_key_table()} WHERE schema = 'staging' AND table = '{ConstantsProvider.get_staging_table(source, table)}'"
        delta_keys_df = presto_sys.execute(query=delta_keys_query)
        delta_keys = ast.literal_eval(delta_keys_df["delta_keys"])
        logger.info(
            f"The latest delta keys of the table {table} with the source {source} are: {delta_keys}"
        )

        delta_load_sql = delta_load_sql.format(**delta_keys)
        logger.info(
            f"The query to retrieve the latest incremental data from table {table} and source {source} is: {delta_load_sql}"
        )

        metadata_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'"
        logger.info(
            f"Getting columns of the table {table} with query: {metadata_query}"
        )
        columns_data = hr_sys.execute(query=metadata_query)

        data_collection = hr_sys.execute(query=delta_load_sql)

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
            ingested_file_name=ConstantsProvider.get_deltaload_ingest_file(),
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hr_sys.disconnect()
        presto_sys.disconnect()


def delta_HDFS_LandingZone_to_Hive_Staging(
    logger: logging.Logger, table_config: dict, source: str
):
    table = table_config.get("table")

    hdfs_sys = HDFSDataHook()
    try:
        hdfs_sys.connect()

        table_schema = hdfs_sys.execute(
            command="data_schema",
            table_name=table,
            source_name=source,
            file_name=ConstantsProvider.get_deltaload_ingest_file(),
            date_str=datetime.now().strftime("%Y-%m-%d"),
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

    hr_sys = HRSystemDataHook()
    try:
        hr_sys.connect()

        existed_data_query = f"""SELECT {",".join(primary_keys)} 
            FROM {ConstantsProvider.get_staging_table(source=source, table=table)} 
            WHERE {ConstantsProvider.soft_delete_meta_field()} = False"""

        logger.info(
            f"Detecting the current existed data in DataWarehouse with {existed_data_query}"
        )
        data_collection = hr_sys.execute(query=existed_data_query)

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
        hr_sys.disconnect()


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
            HDFS_table_location_dir=ConstantsProvider.HDFS_LandingZone_delete_reconcile_base_dir(
                source, table
            ),
            HDFS_partition_location_dir=(
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
    logger: logging.Logger, table_config: dict, source: str, mode: str
):
    table = table_config.get("table")
    delta_keys = table_config.get("delta_keys")
    primary_keys = table_config.get("primary_keys")

    hive_sys = HiveDataHook()
    presto_sys = PrestoDataHook()
    hdfs_sys = HDFSDataHook()
    try:
        hive_sys.connect()

        if mode == "delta":
            reconcile_DDL_hql = f"""CREATE VIEW {ConstantsProvider.get_delta_temp_view_table(source, table)} AS 
                    SELECT t2.* FROM 
                    (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY {",".join(primary_keys)} ORDER BY {",".join(delta_keys)} DESC) rn
                        FROM (
                            SELECT * FROM {ConstantsProvider.get_staging_table(source, table)}
                            UNION ALL
                            SELECT * FROM {ConstantsProvider.get_delta_table(source, table)}
                        ) t1
                    ) t2 WHERE t1.rn = 1
                """

            logger.info(
                f"Creating the view to reconcile the data with query: {reconcile_DDL_hql}"
            )

            reconcile_hql = f"""INSERT OVERWRITE TABLE {ConstantsProvider.get_staging_table(source, table)}
                        SELECT * FROM {ConstantsProvider.get_delta_temp_view_table(source, table)}"""

            drop_reconcile_DDL_hql = f"DROP VIEW {ConstantsProvider.get_delta_temp_view_table(source, table)}"

        elif mode == "recocile_delete":
            presto_sys.connect()
            hdfs_sys.connect()

            table_schema = hdfs_sys.execute(
                command="data_schema",
                table_name=table,
                source_name=source,
                file_name=ConstantsProvider.get_deltaload_ingest_file(),
                date_str=datetime.now().strftime("%Y-%m-%d"),
            )
            selected_cols = map(
                lambda col: (
                    "t1." + col
                    if col != ConstantsProvider.soft_delete_meta_field()
                    else "TRUE AS " + ConstantsProvider.soft_delete_meta_field()
                ),
                table_schema,
            )
            delete_detection_hql = f"""SELECT {",".join(selected_cols)} 
                        FROM {ConstantsProvider.get_staging_table(source, table)} t1
                        WHERE NOT EXISTS (
                            SELECT * FROM {ConstantsProvider.get_reconcile_delete_table(source, table)} t2
                            WHERE {" AND ".join(map(lambda key: "t1." + key + " = " + "t2." + key, primary_keys))}
                        )
                    """

            reconcile_DDL_hql = f"""CREATE VIEW {ConstantsProvider.get_reconcile_delete_temp_view_table(source, table)} AS 
                    SELECT t2.* FROM 
                    (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY {",".join(primary_keys)} ORDER BY {",".join(delta_keys)} DESC) rn
                        FROM (
                            SELECT * FROM {ConstantsProvider.get_staging_table(source, table)}
                            UNION ALL
                            SELECT * FROM ( {delete_detection_hql} )
                        ) t1
                    ) t2 WHERE t1.rn = 1
                """

            logger.info(
                f"Creating the view to reconcile the data with query: {reconcile_DDL_hql}"
            )

            reconcile_hql = f"""INSERT OVERWRITE TABLE {ConstantsProvider.get_staging_table(source, table)}
                        SELECT * FROM {ConstantsProvider.get_reconcile_delete_temp_view_table(source, table)}"""

            drop_reconcile_DDL_hql = f"DROP VIEW {ConstantsProvider.get_reconcile_delete_temp_view_table(source, table)}"

        with hive_sys.connection.cursor() as cursor:
            cursor.execute(reconcile_DDL_hql)
            cursor.execute(reconcile_hql)
            cursor.execute(drop_reconcile_DDL_hql)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hive_sys.disconnect()


def update_delta_keys(logger: logging.Logger, table_config: dict, source: str):
    table = table_config.get("table")
    delta_load_hql = table_config.get("delta_load_hql")

    delta_load_hql = delta_load_hql.format(
        hive_table=ConstantsProvider.get_staging_table(source, table)
    )
    logger.info(f"Getting the latest delta key with the hiveQL: {delta_load_hql}")

    presto_sys = PrestoDataHook()
    presto_sys.connect()
    try:
        delta_keys_df = presto_sys.execute(query=delta_load_hql)

        delta_keys_dict = delta_keys_df.to_dict("records")[-1]
        logger.info(
            f"""The latest delta key from table '{ConstantsProvider.get_staging_table(source, table)}' and schema 'staging': {str(delta_keys_dict)}"""
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
        presto_sys.disconnect()
