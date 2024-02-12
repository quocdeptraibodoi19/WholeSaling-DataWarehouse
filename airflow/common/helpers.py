import os
from datetime import timedelta
from datetime import datetime

from typing import Iterator

import pandas as pd

import logging

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
                "server": "sql1",
                "database": "HumanResourceSystem",
                "username": "sa",
                "password": "@Quoc1234",
            }

    @staticmethod
    def Wholesaling_sys_creds():
        if ConstantsProvider.get_environment() == "local":
            return {
                "server": "sql1",
                "database": "WholeSaling",
                "username": "sa",
                "password": "@Quoc1234",
            }

    @staticmethod
    def Product_sys_creds():
        if ConstantsProvider.get_environment() == "local":
            return {
                "server": "sql1",
                "database": "Product",
                "username": "sa",
                "password": "@Quoc1234",
            }

    @staticmethod
    def Ecomerce_sys_creds():
        if ConstantsProvider.get_environment() == "local":
            return {
                "server": "sql1",
                "database": "Ecomerce",
                "username": "sa",
                "password": "@Quoc1234",
            }

    @staticmethod
    def Hadoop_creds():
        if ConstantsProvider.get_environment() == "local":
            return {"host": "hadoop-master", "port": "9870", "user": "hadoop"}

    @staticmethod
    def hdfs_config():
        if ConstantsProvider.get_environment() == "local":
            return {"host": "hadoop-master", "port": "9001"}

    @staticmethod
    def Presto_Staging_Hive_creds():
        if ConstantsProvider.get_environment() == "local":
            return {
                "host": "hadoop-master",
                "port": 8080,
                "user": "hadoop",
                "catalog": "hive",
                "schema": "staging",
            }

    @staticmethod
    def Staging_Hive_creds():
        if ConstantsProvider.get_environment() == "local":
            # This is the credentials for the python to connect to hiveserver2
            return {
                "host": "hadoop-master",
                "port": 10000,
                "username": "hadoop",
                "database": "staging",
            }

    @staticmethod
    def HR_query_chunksize():
        return 10**6

    @staticmethod
    def HDFS_LandingZone_base_dir(
        source_system: str = None, table_name: str = None, date_str: str = None
    ):
        # This is the path convention for the Hive partitions in HDFS.
        base_path = "/staging/"
        if source_system:
            base_path += f"{source_system}/"

        if table_name:
            base_path += f"{table_name}/"

        if date_str:
            base_path += f"{ConstantsProvider.ingested_meta_field()}={date_str}/"

        return base_path

    @staticmethod
    def ingested_meta_field():
        return "date_partition"

    @staticmethod
    def get_HR_source():
        return "HR_System"

    @staticmethod
    def get_Product_source():
        return "Product_Management_Platform"

    @staticmethod
    def get_WholeSaling_source():
        return "WholeSale_System"

    @staticmethod
    def get_Ecomerce_source():
        return "Ecomerce"

    @staticmethod
    def get_DW_Layer(level: int):
        if level == 0:
            return "landingzone"
        elif level == 1:
            return "staging"
        elif level == 2:
            return "NDS"
        elif level == 3:
            return "analytics"
        else:
            raise ValueError("level is invalid...")

    @staticmethod
    def get_delta_key_table():
        return "delta_keys"

    @staticmethod
    def get_staging_table(source: str, table: str):
        return f"{source.lower()}_{table.lower()}"

    @staticmethod
    def config_file_path(source: str):
        if ConstantsProvider.get_environment() == "local":
            base = "config/"
            opts = {
                ConstantsProvider.get_HR_source(): "hr_system.yaml",
                ConstantsProvider.get_Product_source(): "product_system.yaml",
                ConstantsProvider.get_WholeSaling_source(): "wholesaling_system.yaml",
                ConstantsProvider.get_Ecomerce_source(): "ecomerce_system.yaml",
            }
            return base + opts[source]
    
    @staticmethod
    def standardlize_date_format(data_collection: Iterator[pd.DataFrame], column: str, datetime_format: str, logger: logging.Logger = None):
        for data in data_collection:
            data[column] = pd.to_datetime(data[column], format=datetime_format)
            if logger is not None:
                logger.info(f"Standardlized dateframe: {data[column]}")
            yield data