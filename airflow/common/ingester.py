import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from abc import ABC, abstractmethod
from .helpers import ConstantsProvider

import pyodbc

import pandas as pd

from hdfs import InsecureClient
import prestodb
from pyhive import hive

from typing import Iterable, List
from datetime import datetime

import logging

logger = logging.getLogger(__name__)


class SysDataHook(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def receive_data(self, *args, **kwargs):
        pass

    @abstractmethod
    def move_data(self, *args, **kwargs):
        pass

    @abstractmethod
    def disconnect(self):
        pass


class HRSystemDataHook(SysDataHook):
    def connect(self):
        logger.info("Connecting to HR System...")
        self.creds = ConstantsProvider.HR_sys_creds()
        self.connection = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};\
                    SERVER="
            + self.creds.get("server")
            + ";\
                    DATABASE="
            + self.creds.get("database")
            + ";\
                    UID="
            + self.creds.get("username")
            + ";\
                    PWD="
            + self.creds.get("password")
            + "; \
                    TrustServerCertificate=yes;"
        )

    def disconnect(self):
        logger.info("Disconnecting from HR System...")
        self.connection.close()

    def receive_data(self, query: str, chunksize: int = None, *args, **kwargs):
        logger.info(f"Getting data from query: {query}")
        return pd.read_sql(query, self.connection, chunksize=chunksize)

    def move_data(self, *args, **kwargs):
        return super().move_data(*args, **kwargs)


class HDFSLandingZoneDataHook(SysDataHook):
    def connect(self):
        logger.info("Connecting to HDFS Landing Zone...")
        self.creds = ConstantsProvider.Hadoop_creds()
        self.connection = InsecureClient(
            f'http://{self.creds.get("host")}:{self.creds.get("port")}',
            user=self.creds.get("user"),
        )

    def disconnect(self):
        """
        hdfs lib automatically handle the process of closing connection since the connection itself is wrapped inside the context manager.
        """
        logger.info("Disconnecting from HDFS Landing Zone...")
        return super().disconnect()

    def move_data(
        self,
        source_system: str,
        table_name: str,
        data_collection: Iterable[pd.DataFrame],
        *args,
        **kwargs,
    ):
        i = 0
        for data in data_collection:
            data[ConstantsProvider.ingested_meta_field()] = pd.to_datetime(
                datetime.now().strftime("%Y-%m-%d")
            )

            logger.info(f"Moving data: {data}")

            hdfs_data_path = (
                ConstantsProvider.HDFS_LandingZone_base_dir(
                    source_system, table_name, datetime.now().strftime("%Y-%m-%d")
                )
                + f"ingested_data_{i}.csv"
            )
            logger.info(f"Destination: {hdfs_data_path}")

            with self.connection.write(
                hdfs_path=hdfs_data_path, overwrite=True
            ) as writer:
                data.to_csv(writer, index=False)

            i += 1

    def receive_data(self, *args, **kwargs):
        return super().receive_data(*args, **kwargs)

    def get_data_schema(self, table_name: str, source_name: str, date_str: str):
        with self.connection.read(
            ConstantsProvider.HDFS_LandingZone_base_dir(
                source_name, table_name, date_str
            )
            + "ingested_data_0.csv",
            encoding="utf-8",
        ) as file:
            df = pd.read_csv(file, nrows=1)

        return list(df.columns)


"""
    This is for the interaction between the ELT and Presto but it seems like prestoDB is used for ad-hoc query ...
    so there are many features regarding DDL in Hive that presto does not support for
    => Using it for the ELT seems to not be the good idea
    => Hive: For the ETL/ELT layer.
    => PrestoDB: For the ad-hoc query serving layer, or for the BI tools for the better performance.
"""


class PrestoHiveStagingDataHook(SysDataHook):
    def connect(self):
        logger.info("Connecting to Hive Metastore via Presto SQl Engine...")
        self.creds = ConstantsProvider.Presto_Staging_Hive_creds()
        self.connection = prestodb.dbapi.connect(**self.creds)

    def disconnect(self):
        logger.info("Disconnecting from Presto SQL Engine...")
        self.connection.close()

    def receive_data(self, query: str, chunksize: int = None, *args, **kwargs):
        logger.info(f"Getting data from query: {query}")
        return pd.read_sql(query, self.connection, chunksize=chunksize)

    def move_data(
        self,
        table_name: str,
        source_system: str,
        table_columns: List[str] = None,
        *args,
        **kwargs,
    ):
        hdfs_config = ConstantsProvider.hdfs_config()

        if not self.check_external_table_existence(hive_table_name=table_name):
            logger.info(f"{table_name} hasn't existed on Hive yet...")

            table_schema = list(map(lambda column: column + " VARCHAR", table_columns))
            presto_sql = f"""CREATE TABLE {table_name.lower()} 
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
            logger.info(f"{table_name} has already existed on Hive...")
            logger.info(f"Adding new partition into external table {table_name}...")

            presto_sql = f"""CALL system.create_empty_partition(
                    schema_name => '{self.creds.get("schema")}',
                    table_name => '{table_name.lower()}',
                    partition_columns => ARRAY['{ConstantsProvider.ingested_meta_field()}'],
                    partition_values => ARRAY['{datetime.now().strftime("%Y-%m-%d")}'])
                """

        logger.info(
            f"Creating external table on Hive and update it with partitions via PrestoDB with presto query: {presto_sql}"
        )

        try:
            cursor = self.connection.cursor()
            cursor.execute(presto_sql)
            # Because the above presto sql just exists on the metadata of PrestoDB => need to flush into Hive Metastore for it to aware of that
            flush_query = f"""CALL system.sync_partition_metadata(schema_name=> '{self.creds.get("schema")}', table_name=> '{table_name.lower()}', mode=> 'FULL')"""
            logger.info(
                f"Making Hive Metastore aware of metadata with the SQL on Presto: {flush_query}"
            )
            cursor.execute(flush_query)
        finally:
            cursor.close()

    def check_external_table_existence(self, hive_table_name: str):
        """
        Method to check if a specific table has existed on Hive or not
        """

        logger.info(
            f"Checking for the existence of {hive_table_name.lower()} on Hive..."
        )
        checking_query = f"SHOW TABLES LIKE '{hive_table_name.lower()}'"

        try:
            cursor = self.connection.cursor()
            cursor.execute(checking_query)
            result = cursor.fetchall()
        finally:
            cursor.close()

        return len(result) != 0


class HiveStagingDataHook(SysDataHook):
    def connect(self):
        logger.info("Connecting to Hive Metastore ...")
        self.creds = ConstantsProvider.Staging_Hive_creds()
        self.connection = hive.Connection(**self.creds)

    def disconnect(self):
        logger.info("Disconnecting from Hive Metastore...")
        self.connection.close()

    def move_data(
        self,
        table_name: str,
        source_system: str,
        table_columns: List[str] = None,
        is_full_load : bool = False,
        *args,
        **kwargs,
    ):  
        if is_full_load:    
            drop_ddl = f"""DROP TABLE IF EXISTS {table_name.lower()}"""

            logger.info(f"Dropping the table {table_name} on Hive with query: {drop_ddl}")

            with self.connection.cursor() as cursor:
                cursor.execute(drop_ddl)

        table_schema = list(map(lambda column: column + " STRING", table_columns))
        hive_ddl = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {table_name.lower()} 
                    ( {", ".join(table_schema[:-1])} )
                    PARTITIONED BY ({ConstantsProvider.ingested_meta_field()} STRING)
                    ROW FORMAT DELIMITED
                    FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '{ConstantsProvider.HDFS_LandingZone_base_dir(source_system, table_name)}'
                """

        logger.info(
            f"Creating the external table {table_name} on Hive with the query: {hive_ddl}"
        )

        with self.connection.cursor() as cursor:
            cursor.execute(hive_ddl)

        adding_partition_ddl = f"""ALTER TABLE {table_name.lower()} 
                    ADD PARTITION ({ConstantsProvider.ingested_meta_field()}='{datetime.now().strftime("%Y-%m-%d")}') 
                    LOCATION '{ConstantsProvider.HDFS_LandingZone_base_dir(source_system, table_name, datetime.now().strftime("%Y-%m-%d"))}'
                """

        logger.info(
            f"Suplementing data partition into {table_name} on Hive with the query: {adding_partition_ddl}"
        )

        with self.connection.cursor() as cursor:
                    cursor.execute(adding_partition_ddl)

    def receive_data(self, query: str, chunksize: int = None, *args, **kwargs):
        logger.info(f"Getting data from query: {query}")
        return pd.read_sql(query, self.connection, chunksize=chunksize)
