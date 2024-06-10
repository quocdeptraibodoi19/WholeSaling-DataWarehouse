export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8084

export ENV=local

export DBT_PROJECT_DIR=$(pwd)/dbt_wholesaleDW
export DBT_PROFILES_DIR=$(pwd)/dbt_wholesaleDW

export USER=
export DW_SCHEMA=dw_$USER

export hr_server=sql1
export hr_source=HR_System
export hr_database=HumanResourceSystem
export hr_user=
export hr_pass=

export wholesale_server=sql1
export wholesale_source=WholeSale_System
export wholesale_database=WholeSaling
export wholesale_user=
export wholesale_pass=

export product_server=sql1
export product_source=Product_Management_Platform
export product_database=Product
export product_user=
export product_pass=

export ecom_server=sql1
export ecom_database=Ecomerce
export ecom_source=Ecomerce
export ecom_user=
export ecom_pass=

export hadoop_host=node01
export hadoop_port=9870
export hadoop_user=hdfs

export hdfs_host=$hadoop_host
export hdfs_port=9000

export dw_host=$hadoop_host
export dw_port=10000
export dw_user=hive
export dw_dimension_db=${DW_SCHEMA}_dds
export dw_staging_db=${DW_SCHEMA}_staging

export visualization_opdb_host=
export visualization_opdb_user=
export visualization_opdb_password=
export visualization_opdb_dbname=visualization
export visualization_opdb_port=