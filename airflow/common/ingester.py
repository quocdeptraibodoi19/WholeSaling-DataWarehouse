import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from abc import ABC, abstractmethod
from .helpers import ConstantsProvider

import pyodbc

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from hdfs import InsecureClient
from typing import Iterable
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
        hr_creds = ConstantsProvider.HR_sys_creds()
        self.connection = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};\
                    SERVER="
            + hr_creds.get("server")
            + ";\
                    DATABASE="
            + hr_creds.get("database")
            + ";\
                    UID="
            + hr_creds.get("username")
            + ";\
                    PWD="
            + hr_creds.get("password")
            + "; \
                    TrustServerCertificate=yes;"
        )

    def disconnect(self):
        logger.info("Disconnecting from HR System...")
        self.connection.close()

    def receive_data(self, query: str, chunksize: int = None, *args, **kwargs):
        logger.info(f"Getting data from query: {query}")
        return pd.read_sql(
            query, self.connection, chunksize=chunksize
        )

    def move_data(self, *args, **kwargs):
        return super().move_data(*args, **kwargs)


class HDFSLandingZoneDataHook(SysDataHook):
    def connect(self):
        logger.info("Connecting to HDFS Landing Zone...")
        hadoop_creds = ConstantsProvider.Hadoop_creds()
        self.connection = InsecureClient(
            f'http://{hadoop_creds.get("host")}:{hadoop_creds.get("port")}',
            user=hadoop_creds.get("user"),
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
        for i, data in enumerate(data_collection):
            logger.info(f"Moving data: {data}")

            hdfs_data_path = f'/staging/{source_system}/{table_name}/{datetime.now().strftime("%Y-%m-%d")}/ingested_data_{i}.csv'
            logger.info(f"Destination: {hdfs_data_path}")

            with self.connection.write(
                hdfs_path=hdfs_data_path, overwrite=True
            ) as writer:
                data.to_csv(writer)
    
    def receive_data(self, *args, **kwargs):
        return super().receive_data(*args, **kwargs)
    
# class HiveDWDataHook(SysDataHook):
#     def connect(self):
#         pass
