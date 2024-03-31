import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import pyodbc

from airflow.common.helpers import ConstantsProvider

base_dir = "/root/WholeSaling-DataWarehouse/split_sources/"

def get_credential(source: str):
    if source == "Ecomerce":
        return ConstantsProvider.Ecomerce_sys_creds()
    if source == "HumanResourceSystem":
        return ConstantsProvider.HR_sys_creds()
    if source == "Product":
        return ConstantsProvider.Product_sys_creds()
    if source == "WholeSaling":
        return ConstantsProvider.Wholesaling_sys_creds()

def source_sql_folder(source: str):
    if source == "Ecomerce":
        return "ecomerce"
    if source == "HumanResourceSystem":
        return "hr"
    if source == "Product":
        return "product"
    if source == "WholeSaling":
        return "wholesale"


def split_source(source: str):
    creds = get_credential(source=source)
    server_conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};\
                    SERVER="
            + creds.get("server")
            + ";\
                    DATABASE="
            + creds.get("database")
            + ";\
                    UID="
            + creds.get("username")
            + ";\
                    PWD="
            + creds.get("password")
            + "; \
                    TrustServerCertificate=yes;"
        )
    
    cursor = server_conn.cursor()

    sql_dir = base_dir + source_sql_folder(source)
    sql_files = [f for f in os.listdir(sql_dir) if f.endswith(".sql")]

    for file in sql_files:
        with open(os.path.join(sql_dir, file), "r") as sql_file:
            print(f"Creating the table {file[:-4]} in the source {source}")
            sql_script = sql_file.read()
            sql_command = sql_script.split("GO")[1].strip() # GO is not a valid SQL statement; rather, it's a batch separator used by SQL Server Management Studio,...
            cursor.execute(sql_command)
            server_conn.commit()
    
    cursor.close()
    server_conn.close()


sources = ["Ecomerce", "HumanResourceSystem", "Product", "WholeSaling"]
for source in sources:
    print(f"-------------------------- {source} ---------------------------------- \n")
    split_source(source=source)
