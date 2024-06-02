import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import pyodbc

from airflow.common.helpers import ConstantsProvider

base_dir = os.getcwd()

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
    
def get_table_schema(table_name, cursor):
    cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
    return list(cursor.fetchall())

# Function to generate SQL for creating log table
def generate_log_table_sql(schema, table_name):
    log_table_name = ConstantsProvider.delete_log_table_name(table_name)
    columns = []
    for col in schema:
        column = f"[{col[0]}] {col[1]}"
        if col[1] == 'nvarchar':
            column += "(max)"
        columns.append(column)
    columns = ', '.join(columns)
    return f"CREATE TABLE {log_table_name} ({columns}, deleted_time DATETIME)"

# Function to generate SQL for creating trigger
def generate_trigger_sql(schema, table_name):
    log_table_name = ConstantsProvider.delete_log_table_name(table_name)
    trigger_name = ConstantsProvider.delete_trigger_name(table_name)
    columns = ["[" + str(col[0]) + "]"  for col in schema]
    return f"""
    CREATE TRIGGER {trigger_name}
    ON [{table_name}] 
    AFTER DELETE 
    AS
        INSERT INTO {log_table_name}(deleted_time, {",".join(columns)})
        SELECT GETDATE(), {",".join(columns)}
        FROM Deleted;
    """

def execute_trigger_creation(source: str):
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

    sql_dir = base_dir + "/" + source_sql_folder(source)
    tables = [f[:-4] for f in os.listdir(sql_dir) if f.endswith(".sql")]

    for table in tables:
        print(f"Creating trigger for the table {table} in the source {source}")

        schema = get_table_schema(table, cursor)
        print(f"The schema of table {source}.{table} is: {schema}")

        trigger_sql = generate_trigger_sql(schema=schema, table_name=table)
        print(f"The trigger sql is: {trigger_sql}")

        log_table_sql = generate_log_table_sql(schema=schema, table_name=table)
        print(f"The log table sql is {log_table_sql}")

        log_table_name = ConstantsProvider.delete_log_table_name(table)
        cursor.execute(f"IF OBJECT_ID('{log_table_name}', 'U') IS NOT NULL DROP TABLE {log_table_name}")
        cursor.execute(log_table_sql)

        trigger_name = ConstantsProvider.delete_trigger_name(table)
        cursor.execute(f"IF OBJECT_ID('{trigger_name}', 'TR') IS NOT NULL DROP TRIGGER {trigger_name}")
        cursor.execute(trigger_sql)

    server_conn.commit()
    server_conn.close()

sources = ["Ecomerce", "HumanResourceSystem", "Product", "WholeSaling"]
for source in sources:
    print(f"-------------------------- {source} ---------------------------------- \n")
    execute_trigger_creation(source=source)

    