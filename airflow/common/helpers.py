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