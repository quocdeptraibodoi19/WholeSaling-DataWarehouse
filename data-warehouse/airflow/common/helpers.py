import os
from datetime import timedelta

from typing import Iterator, Callable

import pandas as pd

import logging

import yaml

class ConstantsProvider:
    @staticmethod
    def get_environment():
        if os.environ.get("ENV") == "local":
            return "local"
        else:
            return "prod"

    @staticmethod
    def default_dag_args():
        if ConstantsProvider.get_environment() == "local":
            return {
                "depends_on_past": False,
                "email": ["airflow@example.com"],
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 0,
                "retry_delay": timedelta(minutes=5),
            }

    @staticmethod
    def HR_sys_creds():
        if ConstantsProvider.get_environment() == "local":
            return {
                "server": os.getenv("hr_server"),
                "database": os.getenv("hr_database"),
                "username": os.getenv("hr_user"),
                "password": os.getenv("hr_pass"),
            }

    @staticmethod
    def Wholesaling_sys_creds():
        if ConstantsProvider.get_environment() == "local":
            return {
                "server": os.getenv("wholesale_server"),
                "database": os.getenv("wholesale_database"),
                "username": os.getenv("wholesale_user"),
                "password": os.getenv("wholesale_pass"),
            }

    @staticmethod
    def Product_sys_creds():
        if ConstantsProvider.get_environment() == "local":
            return {
                "server": os.getenv("product_server"),
                "database": os.getenv("product_database"),
                "username": os.getenv("product_user"),
                "password": os.getenv("product_pass"),
            }

    @staticmethod
    def Ecomerce_sys_creds():
        if ConstantsProvider.get_environment() == "local":
            return {
                "server": os.getenv("ecom_server"),
                "database": os.getenv("ecom_database"),
                "username": os.getenv("ecom_user"),
                "password": os.getenv("ecom_pass"),
            }

    @staticmethod
    def Hadoop_creds():
        if ConstantsProvider.get_environment() == "local":
            return {
                "host": os.getenv("hadoop_host"),
                "port": os.getenv("hadoop_port"),
                "user": os.getenv("hadoop_user"),
            }

    @staticmethod
    def HDFS_creds():
        if ConstantsProvider.get_environment() == "local":
            return {"host": os.getenv("hdfs_host"), "port": os.getenv("hdfs_port"), "user": os.getenv("hadoop_user")}

    @staticmethod
    def Presto_Staging_Hive_creds():
        if ConstantsProvider.get_environment() == "local":
            return {
                "host": os.getenv("presto_host"),
                "port": int(os.getenv("presto_port")),
                "user": os.getenv("presto_user"),
                "catalog": os.getenv("presto_catalog"),
                "schema": os.getenv("presto_schema"),
            }

    @staticmethod
    def Staging_Hive_creds():
        if ConstantsProvider.get_environment() == "local":
            # This is the credentials for the python to connect to hiveserver2
            return {
                "host": os.getenv("dw_host"),
                "port": int(os.getenv("dw_port")),
                "username": os.getenv("dw_user"),
                "database": os.getenv("dw_staging_db"),
            }

    @staticmethod
    def SparkSQL_Context_config():
        if ConstantsProvider.get_environment() == "local":
            return {
                "master": os.getenv("spark_master"),
                "appname": "ELT-SPARKAPP",
                "hive.metastore.uris": os.getenv("spark_hive_metastore_uris"),
                "spark.sql.warehouse.dir": os.getenv("spark_sql_warehouse_dir"),
                "spark.default.db": os.getenv("dw_staging_db"),
            }

    @staticmethod
    def HR_query_chunksize():
        return 10**6

    @staticmethod
    def Presto_query_chunksize():
        return 10**6

    @staticmethod
    def delete_trigger_name(table):
        return f"trg_{table}_delete"
    
    @staticmethod
    def delete_log_table_name(table):
        return f"delete_{table}_log"

    @staticmethod
    def delete_date_custom_cast_delete_log():
        return "deleted_time", 'CONVERT(NVARCHAR(MAX), deleted_time, 121)'
    
    @staticmethod
    def HDFS_LandingZone_base_dir(
        source_system: str = None, table_name: str = None, date_str: str = None, is_full_load=True
    ):
        # This is the path convention for the Hive partitions in HDFS.
        base_path = "/staging/" if is_full_load else "/staging/delta_load/"
        if source_system:
            base_path += f"{source_system}/"

        if table_name:
            base_path += f"{table_name}/"

        if date_str:
            base_path += f"{ConstantsProvider.ingested_meta_field()}={date_str}/"

        return base_path

    @staticmethod
    def HDFS_LandingZone_delete_reconcile_base_dir(
        source_system: str = None, table_name: str = None, date_str: str = None
    ):

        base_path = "/delete_reconcile/"
        if source_system:
            base_path += f"{source_system}/"

        if table_name:
            base_path += f"{table_name}/"

        if date_str:
            base_path += f"{ConstantsProvider.ingested_meta_field()}={date_str}/"

        return base_path
    
    @staticmethod
    def HDFS_LandingZone_data_firewall_base_dir(date_str: str):
        return f"/data_firewall/{ConstantsProvider.ingested_meta_field()}={date_str}/"
    
    @staticmethod
    def ingested_meta_field():
        return "extract_date"

    @staticmethod
    def soft_delete_meta_field():
        return "is_deleted"

    @staticmethod
    def get_HR_source():
        return os.getenv("hr_source")

    @staticmethod
    def get_Product_source():
        return os.getenv("product_source")

    @staticmethod
    def get_WholeSaling_source():
        return os.getenv("wholesale_source")

    @staticmethod
    def get_Ecomerce_source():
        return os.getenv("ecom_source")

    @staticmethod
    def get_update_key():
        return 'ModifiedDate'
    
    @staticmethod
    def get_airflow_all_tables_option():
        return "All Tables"

    @staticmethod
    def get_sources_datetime_format_standardization():
        return "%Y-%m-%d %H:%M:%S.%f"

    @staticmethod
    def get_staging_DW_name():
        return os.getenv("dw_staging_db")

    @staticmethod
    def get_delta_key_table():
        return "extraction_metadata"

    @staticmethod
    def get_temp_delta_key_table():
        return "temp_extraction_metadata"

    @staticmethod
    def get_staging_table(source: str, table: str):
        return f"{source.lower()}_{table.lower()}"

    @staticmethod
    def get_delta_table(source: str, table: str):
        return f"delta_{ConstantsProvider.get_staging_table(source, table)}"

    @staticmethod
    def get_reconcile_delete_table(source: str, table: str):
        return f"reconcile_delete_{ConstantsProvider.get_staging_table(source, table)}"

    @staticmethod
    def get_delta_temp_view_table(source: str, table: str):
        return f"temp_{ConstantsProvider.get_delta_table(source, table)}"

    @staticmethod
    def get_reconcile_delete_temp_view_table(source: str, table: str):
        return f"temp_{ConstantsProvider.get_reconcile_delete_table(source, table)}"

    @staticmethod
    def get_delta_reconcile_delete_temp_view_table(source: str, table: str):
        return f"temp_delta_reconcile_delete_{ConstantsProvider.get_staging_table(source, table)}"
    
    @staticmethod
    def get_resolved_DQ_table():
        return "resolved_data_quality"

    @staticmethod
    def get_DQ_table():
        return "data_quality"

    @staticmethod
    def get_DQ_table_schema():
        return ["id", "source"]
    
    @staticmethod
    def get_fullload_ingest_file():
        return "ingested_data_{}.parquet"

    @staticmethod
    def get_deltaload_ingest_file():
        return "delta_ingested_data_{}.parquet"

    @staticmethod
    def get_data_key_ingest_file():
        return "data_keys_{}.csv"

    @staticmethod
    def get_data_firewall_file():
        return "data_firewall_{}.csv"

    @staticmethod
    def config_file_path(source: str):
        if ConstantsProvider.get_environment() == "local":
            base = "config/"
            opts = {
                ConstantsProvider.get_HR_source(): "hr_system.yaml",
                ConstantsProvider.get_Product_source(): "product_sys.yaml",
                ConstantsProvider.get_WholeSaling_source(): "wholesale_sys.yaml",
                ConstantsProvider.get_Ecomerce_source(): "ecomerce_sys.yaml",
            }
            return base + opts[source]


class DataManipulator:
    def __init__(
        self, data_collection: Iterator[pd.DataFrame], logger: logging.Logger
    ) -> None:
        self._data_collection = data_collection
        self._transformations = []
        self._logger = logger

    def transform(
        self, transform_func: Callable[[pd.DataFrame, logging.Logger], pd.DataFrame]
    ):
        self._transformations.append(transform_func)
        return self

    def execute(self):
        for data in self._data_collection:
            for transformation in self._transformations:
                data = transformation(data, self._logger)

            yield data


class DataManipulatingManager:
    @staticmethod
    def standardlize_date_format(column: str, datetime_format: str):
        def transform(data: pd.DataFrame, logger: logging.Logger):

            data[column] = pd.to_datetime(data[column])
            data[column] = data[column].dt.strftime(datetime_format)

            logger.info(
                f"Standarlizing data (column {column}) with format '{datetime_format}' ..."
            )
            logger.info(f"Tunning the dataframe: {data[column]}")

            return data

        return transform

    @staticmethod
    def pd_column_to_string(column: str):
        def transform(data: pd.DataFrame, logger: logging.Logger):

            data[column] = data[column].astype(str)

            logger.info(f"Convert column {column} to string ... ")
            logger.info(f"Tunning the dataframe: {data[column]}")

            return data

        return transform

    @staticmethod
    def add_new_column_data_collection(column: str, val):
        def transform(data: pd.DataFrame, logger: logging.Logger):
            data[column] = str(val)
            logger.info(f"Adding new column {column} with value {val} ...")
            logger.info(f"Tunning the dataframe: {data[column]}")
            return data

        return transform

class SourceConfigHandler():
    def __init__(self, source: str, is_fullload: bool) -> None:
        self.full_load = is_fullload
        self.source = source
        with open(
                ConstantsProvider.config_file_path(source=self.source),
                "r",
            ) as file:
            self.tables_configs = yaml.load(file, yaml.Loader).get("full_load" if self.full_load else "delta_load")

    def get_source(self):
        return self.source
    
    def get_tables_configs(self):
        return self.tables_configs
    
    def is_full_load(self):
        return self.full_load
    
    def get_list_tables(self):
        return [table_config.get("table") for table_config in self.tables_configs]
           