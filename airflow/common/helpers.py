import os
from datetime import timedelta

from datetime import datetime


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
