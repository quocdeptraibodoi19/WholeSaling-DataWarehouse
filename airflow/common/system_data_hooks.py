import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from .helpers import ConstantsProvider

import pyodbc

import pandas as pd

from hdfs import InsecureClient
import prestodb
from pyhive import hive

import logging


class SysDataHook:
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self._connection = None

    def connect(self, *args, **kwargs):
        pass

    def disconnect(self, *args, **kwargs):
        pass

    def execute(self, *args, **kwargs):
        pass

    @property
    def connection(self):
        if self._connection is None:
            raise LookupError(
                "Connection is None ... please connect it to the external system first"
            )
        return self._connection


class HRSystemDataHook(SysDataHook):
    def connect(self, *args, **kwargs):
        self.logger.info("Connecting to HR System...")
        self.creds = ConstantsProvider.HR_sys_creds()
        self._connection = pyodbc.connect(
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

    def disconnect(self, *args, **kwargs):
        self.logger.info("Disconnecting from HR System...")
        self.connection.close()

    def execute(self, query: str, chunksize: int = None, *args, **kwargs):
        self.logger.info(f"Getting data from query: {query}")
        return pd.read_sql(query, self.connection, chunksize=chunksize)


class HDFSDataHook(SysDataHook):
    def connect(self, *args, **kwargs):
        self.logger.info("Connecting to HDFS Landing Zone...")
        self.creds = ConstantsProvider.Hadoop_creds()
        self._connection = InsecureClient(
            f'http://{self.creds.get("host")}:{self.creds.get("port")}',
            user=self.creds.get("user"),
        )

    def disconnect(self, *args, **kwargs):
        """
        hdfs lib automatically handle the process of closing connection since the connection itself is wrapped inside the context manager.
        """
        self.logger.info("Disconnecting from HDFS Landing Zone...")
        return super().disconnect()

    def execute(self, command: str, *args, **kwargs):
        """
        Interacting with the HDFS:
            - command = "data_schema": to get data schema from an ingested file in HDFS
        """
        if command == "data_schema":
            return self._get_data_schema(
                table_name=kwargs.get("table_name"),
                source_name=kwargs.get("source_name"),
                date_str=kwargs.get("date_str"),
                file_name=kwargs.get("file_name"),
            )

    def _get_data_schema(
        self, table_name: str, source_name: str, date_str: str, file_name: str
    ):
        with self.connection.read(
            ConstantsProvider.HDFS_LandingZone_base_dir(
                source_name, table_name, date_str
            )
            + file_name.format(0),
            encoding="utf-8",
        ) as file:
            df = pd.read_csv(file, nrows=1, sep="|")

        return list(df.columns)


"""
    This is for the interaction between the ELT and Presto but it seems like prestoDB is used for ad-hoc query ...
    so there are many features regarding DDL in Hive that presto does not support for
    => Using it for the ELT seems to not be the good idea
    => Hive: For the ETL/ELT layer.
    => PrestoDB: For the ad-hoc query serving layer, or for the BI tools for the better performance.
"""


class PrestoDataHook(SysDataHook):
    def connect(self, catalog: str = None, schema: str = None, *args, **kwargs):
        self.logger.info("Connecting to Hive Metastore via Presto SQl Engine...")
        self.creds = ConstantsProvider.Presto_Staging_Hive_creds()

        if schema is not None:
            self.creds["schema"] = schema

        if catalog is not None:
            self.creds["catalog"] = catalog

        self._connection = prestodb.dbapi.connect(**self.creds)

    def disconnect(self, *args, **kwargs):
        self.logger.info("Disconnecting from Presto SQL Engine...")
        self.connection.close()

    def execute(self, query: str, chunksize: int = None, *args, **kwargs):
        self.logger.info(f"Getting data from query: {query}")
        return pd.read_sql(query, self.connection, chunksize=chunksize)

    def check_external_table_existence(self, hive_table_name: str):
        """
        Method to check if a specific table has existed on Hive or not
        """

        self.logger.info(
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


class HiveDataHook(SysDataHook):
    def connect(self, database: str = None, *args, **kwargs):
        self.logger.info("Connecting to Hive Metastore ...")
        self.creds = ConstantsProvider.Staging_Hive_creds()

        if database is not None:
            self.creds["database"] = database

        self._connection = hive.Connection(**self.creds)

    def disconnect(self, *args, **kwargs):
        self.logger.info("Disconnecting from Hive Metastore...")
        self.connection.close()

    def execute(self, query: str, chunksize: int = None, *args, **kwargs):
        self.logger.info(f"Getting data from query: {query}")
        return pd.read_sql(query, self.connection, chunksize=chunksize)