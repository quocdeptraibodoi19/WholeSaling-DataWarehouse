import sys
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from .helpers import ConstantsProvider


import pandas as pd

from typing import Iterable, List
from datetime import datetime

import logging

from .system_data_hooks import HDFSDataHook, PrestoDataHook, HiveDataHook


class DataIngestionStrategy:
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def move_data(self, *args, **kwargs):
        pass


class HDFSLandingZoneIngestionStrategy(DataIngestionStrategy):
    def __init__(self) -> None:
        super().__init__()
        self.data_hook = HDFSDataHook()

    def move_data(
        self,
        source_system: str,
        table_name: str,
        data_collection: Iterable[pd.DataFrame],
        ingested_file_name: str,
        HDFS_location_dir: str = None,
        *args,
        **kwargs,
    ):
        try:
            self.data_hook.connect()
            i = 0
            for data in data_collection:

                self.logger.info(f"Moving data: {data}")

                base_dir = (
                    HDFS_location_dir
                    if HDFS_location_dir is not None
                    else ConstantsProvider.HDFS_LandingZone_base_dir(
                        source_system, table_name, datetime.now().strftime("%Y-%m-%d")
                    )
                )

                hdfs_data_path = base_dir + ingested_file_name.format(i)
                self.logger.info(f"Destination: {hdfs_data_path}")

                with self.data_hook.connection.write(
                    hdfs_path=hdfs_data_path, overwrite=True
                ) as writer:
                    data.to_csv(writer, index=False, sep="|")

                i += 1
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            raise
        finally:
            self.data_hook.disconnect()


class PrestoHiveStagingIngestionStrategy(DataIngestionStrategy):
    def __init__(self) -> None:
        super().__init__()
        self.data_hook = PrestoDataHook()

    def move_data(
        self,
        table_name: str,
        source_system: str,
        table_columns: List[str] = None,
        *args,
        **kwargs,
    ):
        try:
            self.data_hook.connect()

            hdfs_config = ConstantsProvider.hdfs_config()

            if not self.data_hook.check_external_table_existence(
                hive_table_name=table_name
            ):
                self.logger.info(
                    f"{ConstantsProvider.get_staging_table(source_system, table_name)} hasn't existed on Hive yet..."
                )

                table_schema = list(
                    map(lambda column: column + " VARCHAR", table_columns)
                )
                presto_sql = f"""CREATE TABLE {ConstantsProvider.get_staging_table(source_system, table_name)} 
                        ( {", ".join(table_schema)} )
                        WITH (
                                format = 'CSV',
                                external_location = 'hdfs://{hdfs_config.get("host")}:{hdfs_config.get("port")}/{ConstantsProvider.HDFS_LandingZone_base_dir(source_system, table_name)}',
                                partitioned_by = ARRAY['{ConstantsProvider.ingested_meta_field()}']
                        )
                        """
            else:
                # TODO: There is an error regarding the fact that we can not manipulate the external data hive directly from PrestoDB due to the lack of the awareness about the metadata in Presto.
                # TODO: Maybe everytime ingestion happens, create the external table on Hive as a temp file and then insert it into the internal table on PrestoDB (which is stored on Hive but still managed by Presto)
                # => This may incur the data redunancy, which costs us a lot if the amount of data is huge.
                self.logger.info(
                    f"{ConstantsProvider.get_staging_table(source_system, table_name)} has already existed on Hive..."
                )
                self.logger.info(
                    f"Adding new partition into external table {ConstantsProvider.get_staging_table(source_system, table_name)}..."
                )

                presto_sql = f"""CALL system.create_empty_partition(
                        schema_name => '{self.creds.get("schema")}',
                        table_name => '{ConstantsProvider.get_staging_table(source_system, table_name)}',
                        partition_columns => ARRAY['{ConstantsProvider.ingested_meta_field()}'],
                        partition_values => ARRAY['{datetime.now().strftime("%Y-%m-%d")}'])
                    """

            self.logger.info(
                f"Creating external table on Hive and update it with partitions via PrestoDB with presto query: {presto_sql}"
            )

            try:
                cursor = self.data_hook.connection.cursor()
                cursor.execute(presto_sql)
                # Because the above presto sql just exists on the metadata of PrestoDB => need to flush into Hive Metastore for it to aware of that
                flush_query = f"""CALL system.sync_partition_metadata(schema_name=> '{self.creds.get("schema")}', table_name=> '{ConstantsProvider.get_staging_table(source_system, table_name)}', mode=> 'FULL')"""
                self.logger.info(
                    f"Making Hive Metastore aware of metadata with the SQL on Presto: {flush_query}"
                )
                cursor.execute(flush_query)
            finally:
                cursor.close()
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
        HDFS_partition_location_dir: str = None,
        *args,
        **kwargs,
    ):
        try:
            self.data_hook.connect()

            if HDFS_table_location_dir is None and HDFS_partition_location_dir is None:
                HDFS_table_location_dir = ConstantsProvider.HDFS_LandingZone_base_dir(
                    source_system, table_name
                )
                HDFS_partition_location_dir = (
                    ConstantsProvider.HDFS_LandingZone_base_dir(
                        source_system, table_name, datetime.now().strftime("%Y-%m-%d")
                    )
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
                    filter(
                        lambda col: col != ConstantsProvider.ingested_meta_field(),
                        table_columns,
                    ),
                )
            )
            hive_ddl = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {hive_table_name} 
                        ( {", ".join(table_schema)} )
                        PARTITIONED BY ({ConstantsProvider.ingested_meta_field()} STRING)
                        ROW FORMAT DELIMITED
                        FIELDS TERMINATED BY '|'
                        STORED AS TEXTFILE
                        LOCATION '{HDFS_table_location_dir}'
                        TBLPROPERTIES ("skip.header.line.count"="1")
                    """

            self.logger.info(
                f"Creating the external table {hive_table_name} on Hive with the query: {hive_ddl}"
            )

            with self.data_hook.connection.cursor() as cursor:
                cursor.execute(hive_ddl)

            adding_partition_ddl = f"""ALTER TABLE {hive_table_name} 
                        ADD PARTITION ({ConstantsProvider.ingested_meta_field()}='{datetime.now().strftime("%Y-%m-%d")}') 
                        LOCATION '{HDFS_partition_location_dir}'
                    """

            self.logger.info(
                f"Suplementing data partition into {hive_table_name} on Hive with the query: {adding_partition_ddl}"
            )

            with self.data_hook.connection.cursor() as cursor:
                cursor.execute(adding_partition_ddl)
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
        delta_keys_dict: dict,
        *args,
        **kwargs,
    ):
        try:
            self.data_hook.connect()

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
                "schema": "staging",
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
            delta_table_ddl = f"""CREATE TABLE {ConstantsProvider.get_temp_delta_key_table()} AS
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


class DataIngester:
    def __init__(self, ingestion_strategy: DataIngestionStrategy) -> None:
        self.ingestion_strategy = ingestion_strategy

    def ingest(self, *args, **kwargs):
        self.ingestion_strategy.move_data(*args, **kwargs)
