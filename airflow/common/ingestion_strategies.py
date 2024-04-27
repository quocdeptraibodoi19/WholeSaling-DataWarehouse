import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from .helpers import ConstantsProvider


import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from typing import Iterable, List
from datetime import datetime

import logging

from .system_data_hooks import HDFSDataHook, HiveDataHook, SparkSQLDataHook


class DataIngestionStrategy:
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def move_data(self, *args, **kwargs):
        pass


class HDFSLandingZoneIngestionStrategy(DataIngestionStrategy):
    def __init__(self) -> None:
        super().__init__()
        self.data_hook = HDFSDataHook().get_pyarrow_hdfs_connection

    def move_data(
        self,
        source_system: str,
        table_name: str,
        data_collection: Iterable[pd.DataFrame],
        ingested_file_name: str,
        HDFS_location_dir: str = None,
        is_full_load: bool = True,
        *args,
        **kwargs,
    ):
        try:
            i = 0
            for data in data_collection:

                self.logger.info(f"Moving data: {data}")
                adf = pa.Table.from_pandas(data)

                base_dir = (
                    HDFS_location_dir
                    if HDFS_location_dir is not None
                    else ConstantsProvider.HDFS_LandingZone_base_dir(
                        source_system, table_name, datetime.now().strftime("%Y-%m-%d"), is_full_load
                    )
                )

                hdfs_data_path = base_dir + ingested_file_name.format(i)
                self.logger.info(f"Destination: {hdfs_data_path}")

                with self.data_hook.open(
                    hdfs_data_path, "wb", overwrite=True
                ) as writer:
                    pq.write_table(adf, writer)

                i += 1

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            raise
        finally:
            self.data_hook.disconnect()


class HiveStagingIntestionStrategy(DataIngestionStrategy):
    def __init__(self) -> None:
        super().__init__()
        self.data_hook = HiveDataHook()

    def move_data(
        self,
        table_name: str,
        source_system: str,
        hive_table_name: str,
        table_columns: List[str] = None,
        HDFS_table_location_dir: str = None,
        is_full_load: bool = True,
        *args,
        **kwargs,
    ):
        try:
            self.data_hook.connect()

            if HDFS_table_location_dir is None:
                HDFS_table_location_dir = ConstantsProvider.HDFS_LandingZone_base_dir(
                    source_system, table_name, datetime.now().strftime("%Y-%m-%d"), is_full_load
                )

            drop_ddl = f"""DROP TABLE IF EXISTS {hive_table_name}"""

            self.logger.info(
                f"Dropping the table {hive_table_name} on Hive with query: {drop_ddl}"
            )

            with self.data_hook.connection.cursor() as cursor:
                cursor.execute(drop_ddl)

            table_schema = list(
                map(
                    lambda column: (
                        "`" + column + "`" + " STRING"
                        if column
                        not in ConstantsProvider.get_HR_date_fields_for_standardization()
                        else "`" + column + "`" + " TIMESTAMP"
                    ),
                    table_columns,
                )
            )

            schema_str = ', '.join(table_schema)

            hive_ddl = f"""CREATE TABLE {hive_table_name} ({schema_str})
                        USING CSV
                        OPTIONS (
                        path "{HDFS_table_location_dir}",
                        delimiter "|",
                        header "true"
                        )  
                    """
            
            with self.data_hook.connection.cursor() as cursor:
                cursor.execute(hive_ddl)

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            raise
        finally:
            self.data_hook.disconnect()


class HiveStagingDeltaKeyIngestionStrategy(DataIngestionStrategy):
    def __init__(self) -> None:
        super().__init__()
        self.data_hook = HiveDataHook()

    def move_data(
        self,
        source: str,
        table: str,
        LSET: str,
        *args,
        **kwargs,
    ):
        try:
            self.data_hook.connect()

            create_ddl = f"""CREATE TABLE IF NOT EXISTS `{ConstantsProvider.get_delta_key_table()}` 
                ( 
                    `schema` STRING,
                    `table` STRING,
                    `LSET` STRING,
                    {ConstantsProvider.ingested_meta_field()} TIMESTAMP
                )
                STORED AS ORC
            """

            delta_key_config = {
                "schema": f"{ConstantsProvider.get_staging_DW_name()}",
                "table": ConstantsProvider.get_staging_table(source, table),
                "LSET": LSET,
                ConstantsProvider.ingested_meta_field(): str(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ),
            }

            self.logger.info(f"The current delta_config: {delta_key_config}")

            constructed_cols_ddl = map(
                lambda data: (
                    '"' + data[1] + '" AS ' + "`" + data[0] + "`"
                    if data[0] != ConstantsProvider.ingested_meta_field()
                    else 'CAST("' + data[1] + '" AS TIMESTAMP)' + "`" + data[0] + "`"
                ),
                delta_key_config.items(),
            )
            delta_table_ddl = f"""CREATE TABLE IF NOT EXISTS {ConstantsProvider.get_temp_delta_key_table()} AS
                SELECT {",".join(map(lambda col: "t2." + "`" + col + "`", delta_key_config.keys()))} FROM 
                    (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY `schema`, `table` ORDER BY `{ConstantsProvider.ingested_meta_field()}` DESC) rn
                        FROM (
                            SELECT * FROM {ConstantsProvider.get_delta_key_table()}
                            UNION ALL
                            SELECT * FROM ( SELECT 
                                {",".join(constructed_cols_ddl)} 
                            ) subquery
                        ) t1
                    ) t2 WHERE t2.rn = 1
                """

            insert_hql = f"""INSERT OVERWRITE TABLE {ConstantsProvider.get_delta_key_table()} 
                SELECT * FROM {ConstantsProvider.get_temp_delta_key_table()}"""

            delta_table_drop_ddl = (
                f"DROP TABLE {ConstantsProvider.get_temp_delta_key_table()}"
            )

            with self.data_hook.connection.cursor() as cursor:
                self.logger.info(
                    f"Creating table {ConstantsProvider.get_delta_key_table()} with hiveQL: {create_ddl}"
                )
                cursor.execute(create_ddl)

                self.logger.info(
                    f"""Creating table {ConstantsProvider.get_temp_delta_key_table()} with hiveQL: {delta_table_ddl} """
                )
                cursor.execute(delta_table_ddl)

                self.logger.info(
                    f"""Insert data from {ConstantsProvider.get_temp_delta_key_table()} to {ConstantsProvider.get_delta_key_table()}: {insert_hql} """
                )
                cursor.execute(insert_hql)

                self.logger.info(
                    f"""Drop {ConstantsProvider.get_temp_delta_key_table()} with hiveQL: {delta_table_drop_ddl} """
                )
                cursor.execute(delta_table_drop_ddl)

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            raise
        finally:
            self.data_hook.disconnect()


class SparkSQLDeltaKeyIngestionStrategy(DataIngestionStrategy):
    def __init__(self) -> None:
        super().__init__()
        self.data_hook = SparkSQLDataHook()

    def move_data(
        self,
        source: str,
        table: str,
        delta_keys_dict: dict,
        *args,
        **kwargs,
    ):
        try:
            self.data_hook.connect(spark_app_name=f"DeltaKeyIngestion_{source}_{table}")

            create_ddl = f"""CREATE TABLE IF NOT EXISTS `{ConstantsProvider.get_delta_key_table()}` 
                ( 
                    `schema` STRING,
                    `table` STRING,
                    `delta_keys` STRING,
                    {ConstantsProvider.ingested_meta_field()} TIMESTAMP
                )
                STORED AS ORC
            """

            delta_key_config = {
                "schema": f"{ConstantsProvider.get_staging_DW_name()}",
                "table": ConstantsProvider.get_staging_table(source, table),
                "delta_keys": str(delta_keys_dict),
                ConstantsProvider.ingested_meta_field(): str(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ),
            }

            self.logger.info(f"The current delta_config: {delta_key_config}")

            constructed_cols_ddl = map(
                lambda data: (
                    '"' + data[1] + '" AS ' + "`" + data[0] + "`"
                    if data[0] != ConstantsProvider.ingested_meta_field()
                    else 'CAST("' + data[1] + '" AS TIMESTAMP)' + "`" + data[0] + "`"
                ),
                delta_key_config.items(),
            )
            delta_table_ddl = f"""CREATE TABLE IF NOT EXISTS {ConstantsProvider.get_temp_delta_key_table()} AS
                SELECT {",".join(map(lambda col: "t2." + "`" + col + "`", delta_key_config.keys()))} FROM 
                    (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY `schema`, `table` ORDER BY `{ConstantsProvider.ingested_meta_field()}` DESC) rn
                        FROM (
                            SELECT * FROM {ConstantsProvider.get_delta_key_table()}
                            UNION ALL
                            SELECT * FROM ( SELECT 
                                {",".join(constructed_cols_ddl)} 
                            ) subquery
                        ) t1
                    ) t2 WHERE t2.rn = 1
                """

            insert_hql = f"""INSERT OVERWRITE TABLE {ConstantsProvider.get_delta_key_table()} 
                SELECT * FROM {ConstantsProvider.get_temp_delta_key_table()}"""

            delta_table_drop_ddl = (
                f"DROP TABLE {ConstantsProvider.get_temp_delta_key_table()}"
            )

            self.logger.info(
                f"Creating table {ConstantsProvider.get_delta_key_table()} with hiveQL: {create_ddl}"
            )
            self.data_hook.connection.sql(create_ddl)

            self.logger.info(
                f"""Creating table {ConstantsProvider.get_temp_delta_key_table()} with hiveQL: {delta_table_ddl} """
            )
            self.data_hook.connection.sql(delta_table_ddl)

            self.logger.info(
                f"""Insert data from {ConstantsProvider.get_temp_delta_key_table()} to {ConstantsProvider.get_delta_key_table()}: {insert_hql} """
            )
            self.data_hook.connection.sql(insert_hql)

            self.logger.info(
                f"""Drop {ConstantsProvider.get_temp_delta_key_table()} with hiveQL: {delta_table_drop_ddl} """
            )
            self.data_hook.connection.sql(delta_table_drop_ddl)

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            raise
        finally:
            self.data_hook.disconnect()


class DataIngester:
    def __init__(self, ingestion_strategy: DataIngestionStrategy) -> None:
        self.ingestion_strategy = ingestion_strategy

    def ingest(self, *args, **kwargs):
        self.ingestion_strategy.move_data(*args, **kwargs)
