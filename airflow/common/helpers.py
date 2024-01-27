import os
from datetime import timedelta


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
                "retries": 1,
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
    def HR_query_chunksize():
        return 10**6
