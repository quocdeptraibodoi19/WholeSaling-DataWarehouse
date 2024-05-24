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
        self.data_hook = HDFSDataHook()

    def move_data(
        self,
        data_collection: Iterable[pd.DataFrame],
        ingested_file_name: str,
        source_system: str = None,
        table_name: str = None,
        HDFS_location_dir: str = None,
        is_full_load: bool = True,
        *args,
        **kwargs,
    ):
        try:
            i = 0
            for data in data_collection:

                self.logger.info(f"Moving data: {data}")

                data = data.astype(str)
                adf = pa.Table.from_pandas(data)
                self.logger.info(f"The pyarrow schema of the data is: {adf.schema}")

                base_dir = (
                    HDFS_location_dir
                    if HDFS_location_dir is not None
                    else ConstantsProvider.HDFS_LandingZone_base_dir(
                        source_system, table_name, datetime.now().strftime("%Y-%m-%d"), is_full_load
                    )
                )

                hdfs_data_path = base_dir + ingested_file_name.format(i)
                self.logger.info(f"Destination: {hdfs_data_path}")

                self.data_hook.execute(command="create_parquet_file")(
                    pyarrow_df=adf,
                    hdfs_data_path=hdfs_data_path,
                )

                i += 1

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            raise

class HiveStagingIntestionStrategy(DataIngestionStrategy):
    def __init__(self) -> None:
        super().__init__()
        self.data_hook = HiveDataHook()

    def move_data(
        self,
        hive_table_name: str,
        table_name: str = None,
        source_system: str = None,
        table_columns: List[str] = None,
        HDFS_table_location_dir: str = None,
        is_full_load: bool = True,
        is_log: bool = False,
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
                    ),
                    table_columns,
                )
            )

            schema_str = ', '.join(table_schema)

            hive_ddl = f"""CREATE TABLE {hive_table_name} ({schema_str})
                        STORED AS PARQUET
                        LOCATION '{HDFS_table_location_dir}'
                    """
            
            if is_log:
                delta_table_ddl = f"CONVERT TO DELTA {hive_table_name} NO STATISTICS"

            with self.data_hook.connection.cursor() as cursor:
                cursor.execute(hive_ddl)
                if is_log:
                    cursor.execute(delta_table_ddl)

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
        is_log: str = "0",
        *args,
        **kwargs,
    ):
        try:
            self.data_hook.connect()

            delta_key_config = {
                "source": source.lower(),
                "table": table.lower(),
                "is_log": is_log,
                "LSET": LSET,
                ConstantsProvider.ingested_meta_field(): str(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ),
            }

            self.logger.info(f"The current delta_config: {delta_key_config}")

            constructed_cols_ddl = map(
                lambda data: (
                    '"' + data[1] + '" AS ' + "`" + data[0] + "`"
                ),
                delta_key_config.items(),
            )
            delta_table_ddl = f"""CREATE TABLE IF NOT EXISTS {ConstantsProvider.get_temp_delta_key_table()} AS
                SELECT {",".join(map(lambda col: "t2." + "`" + col + "`", delta_key_config.keys()))} FROM 
                    (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY `source`, `table`, `is_log` ORDER BY `{ConstantsProvider.ingested_meta_field()}` DESC) rn
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
