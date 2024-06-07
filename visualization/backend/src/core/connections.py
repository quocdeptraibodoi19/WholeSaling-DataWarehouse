from pyhive.hive import connect as hive_connect
import os
from psycopg2 import connect as psycopg2_connect, sql


class Connection:
    def __init__(self) -> None:
        self._conn = None

    def connect(self):
        raise NotImplemented("Subclasses should override this method.")
    
    @property
    def conn(self):
        if self._conn is None:
            raise LookupError(
                "Connection is None ... please connect it to the external system first"
            )
        return self._conn

    def disconnect(self):
        if self._conn is not None:
            self._conn.close()


class DataWarehouseConnection(Connection):
    def __init__(self) -> None:
        super().__init__()
        self.creds = {
            "host": os.getenv("dw_host"),
            "port": int(os.getenv("dw_port")),
            "username": os.getenv("dw_user"),
            "database": os.getenv("dw_dimension_db"),
        }

    def connect(self):
        if self._conn is None:
            self._conn = hive_connect(**self.creds)
        return self._conn


class OperationalDBConnection(Connection):
    def __init__(self) -> None:
        super().__init__()
        self.creds = {
            "dbname": os.getenv("visualization_opdb_dbname"),
            "user": os.getenv("visualization_opdb_user"),
            "password": os.getenv("visualization_opdb_password"),
            "host": os.getenv("visualization_opdb_host"),
            "port": int(os.getenv("visualization_opdb_port")),
        }

    def connect(self):
        if self._conn is None:
            self._conn = psycopg2_connect(**self.creds)
        return self._conn

    @staticmethod
    def get_postgres_sql(query: str):
        return sql.SQL(query)