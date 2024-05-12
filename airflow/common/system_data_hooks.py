import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from .helpers import ConstantsProvider

import pyodbc

import pandas as pd

from hdfs import InsecureClient

from pyhive import hive
from pyspark.sql import SparkSession

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

import logging

from datetime import datetime


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

    def execute(
        self,
        query: str,
        chunksize: int = None,
        *args,
        **kwargs,
    ):
        self.logger.info(f"Getting data from query: {query}")
        return pd.read_sql(query, self.connection, chunksize=chunksize)


class ProductSystemDataHook(SysDataHook):
    def connect(self, *args, **kwargs):
        self.logger.info("Connecting to Product System...")
        self.creds = ConstantsProvider.Product_sys_creds()
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
        self.logger.info("Disconnecting from Product System...")
        self.connection.close()

    def execute(
        self,
        query: str,
        chunksize: int = None,
        *args,
        **kwargs,
    ):
        self.logger.info(f"Getting data from query: {query}")
        return pd.read_sql(query, self.connection, chunksize=chunksize)


class WholeSaleSystemDataHook(SysDataHook):
    def connect(self, *args, **kwargs):
        self.logger.info("Connecting to WholeSale System...")
        self.creds = ConstantsProvider.Wholesaling_sys_creds()
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
        self.logger.info("Disconnecting from WholeSale System...")
        self.connection.close()

    def execute(
        self,
        query: str,
        chunksize: int = None,
        *args,
        **kwargs,
    ):
        self.logger.info(f"Getting data from query: {query}")
        return pd.read_sql(query, self.connection, chunksize=chunksize)


class EcommerceSystemDataHook(SysDataHook):
    def connect(self, *args, **kwargs):
        self.logger.info("Connecting to Ecommerce System...")
        self.creds = ConstantsProvider.Ecomerce_sys_creds()
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
        self.logger.info("Disconnecting from Ecommerce System...")
        self.connection.close()

    def execute(
        self,
        query: str,
        chunksize: int = None,
        *args,
        **kwargs,
    ):
        self.logger.info(f"Getting data from query: {query}")
        return pd.read_sql(query, self.connection, chunksize=chunksize)


class HDFSDataHook(SysDataHook):
    def __init__(self):
        super().__init__()
        self.creds = ConstantsProvider.Hadoop_creds()
        self.hdfs_creds = ConstantsProvider.HDFS_creds()
        self._pyarrow_hdfs_conn = None

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
            return self._get_data_schema
        if command == "create_parquet_file":
            return self._create_parquet_file

    def _get_data_schema(
        self,
        table_name: str = None,
        source_name: str = None,
        file_name: str = None,
        date_str: str = datetime.now().strftime("%Y-%m-%d"),
        base_dir: str = None,
        is_full_load: bool = True,
    ):
        if base_dir is None:
            base_dir = ConstantsProvider.HDFS_LandingZone_base_dir(
                source_name, table_name, date_str, is_full_load
            )
        hdfs = fs.HadoopFileSystem(
            host=self.hdfs_creds.get("host"),
            port=int(self.hdfs_creds.get("port")),
            user=self.hdfs_creds.get("user"),
        )
        schema = pq.read_schema(base_dir + file_name.format(0), filesystem=hdfs)
        return [col.name for col in schema]

    def _create_parquet_file(
        self,
        pyarrow_df,
        hdfs_data_path,
    ):
        hdfs_conn = pa.hdfs.connect(
            host=self.hdfs_creds.get("host"),
            port=int(self.hdfs_creds.get("port")),
            user=self.hdfs_creds.get("user"),
        )
        with hdfs_conn.open(hdfs_data_path, "wb") as writer:
            pq.write_table(pyarrow_df, writer)

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

    def execute(
        self,
        query: str,
        chunksize: int = None,
        *args,
        **kwargs,
    ):
        self.logger.info(f"Getting data from query: {query}")
        return pd.read_sql(query, self.connection, chunksize=chunksize)


class SparkSQLDataHook(SysDataHook):
    def connect(
        self, spark_app_name: str = None, database: str = None, *args, **kwargs
    ):
        self.logger.info("Connecting to SparkSQL Driver ...")

        self.config = ConstantsProvider.SparkSQL_Context_config()
        self._connection = (
            SparkSession.builder.appName(
                spark_app_name
                if spark_app_name is not None
                else self.config.get("appname")
            )
            .master(self.config.get("master"))
            .config("hive.metastore.uris", self.config.get("hive.metastore.uris"))
            .config(
                "spark.sql.warehouse.dir", self.config.get("spark.sql.warehouse.dir")
            )
            .enableHiveSupport()
            .getOrCreate()
        )

        if database is not None:
            self._connection.sql(f"use {database}")
        else:
            self._connection.sql(f"""use {self.config.get("spark.default.db")}""")

    def disconnect(self, *args, **kwargs):
        self.logger.info("Disconnecting from SparkSQL...")
        self.connection.stop()

    def execute(
        self,
        query: str,
        *args,
        **kwargs,
    ):
        self.logger.info(f"Getting data from query: {query}")
        return self.connection.sql(query)
    
def get_system_datahook(source: str):
    if ConstantsProvider.get_HR_source() == source:
        return HRSystemDataHook()
    elif ConstantsProvider.get_Ecomerce_source() == source:
        return EcommerceSystemDataHook()
    elif ConstantsProvider.get_WholeSaling_source() == source:
        return WholeSaleSystemDataHook()
    elif ConstantsProvider.get_Product_source() == source:
        return ProductSystemDataHook()
