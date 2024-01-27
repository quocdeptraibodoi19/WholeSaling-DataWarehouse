from abc import ABC, abstractmethod
from helpers import ConstantsProvider

import pyodbc

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from hdfs import InsecureClient
from typing import Iterable
from datetime import datetime


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
        hr_creds = ConstantsProvider.HR_sys_creds()
        self.connection = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};\
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
        self.connection.close()

    def receive_data(self, query: str, *args, **kwargs):
        return pd.read_sql(
            query, self.connection, chunksize=ConstantsProvider.HR_query_chunksize()
        )


class HDFSLandingZoneDataHook(SysDataHook):
    def connect(self):
        hadoop_creds = ConstantsProvider.Hadoop_creds()
        self.connection = InsecureClient(
            f'http://{hadoop_creds.get("host")}:{hadoop_creds.get("port")}',
            user=hadoop_creds.get("user"),
        )

    def disconnect(self):
        """
        hdfs lib automatically handle the process of closing connection since the connection itself is wrapped inside the context manager.
        """
        pass

    def move_data(
        self,
        source_system: str,
        table_name: str,
        data_collection: Iterable[pd.DataFrame],
        is_partitioned=False,
        *args,
        **kwargs,
    ):
        if is_partitioned:
            for i, data in enumerate(data_collection):
                arrow_table = pa.Table.from_pandas(data)

                hdfs_data_path = f'staging/{source_system}/{table_name}/{datetime.now().strftime("%Y-%m-%d")}/ingested_data_{i}.parquet'

                with self.connection.write(hdfs_path=hdfs_data_path, overwrite=True) as writer:
                    pq.write_table(arrow_table, writer)
        else:
            hdfs_data_path = f'staging/{source_system}/{table_name}/{datetime.now().strftime("%Y-%m-%d")}/ingested_data.parquet'
            for data in data_collection:
                arrow_table = pa.Table.from_pandas(data)

                with self.connection.write(hdfs_path=hdfs_data_path, append=True) as writer:
                    pq.write_table(arrow_table, writer)


class HiveDWDataHook(SysDataHook):
    def connect(self):
        pass
