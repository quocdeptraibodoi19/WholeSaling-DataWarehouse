export AIRFLOW_HOME=$(pwd)/airflow

export ENV=local

export DBT_PROJECT_DIR=$(pwd)/dbt_wholesaleDW
export DBT_PROFILES_DIR=$(pwd)/dbt_wholesaleDW

export USER=quok

export hr_server=sql1
export hr_database=HumanResourceSystem
export hr_user=
export hr_pass=

export wholesale_server=sql1
export wholesale_database=WholeSaling
export wholesale_user=
export wholesale_pass=

export product_server=sql1
export product_database=Product
export product_user=
export product_pass=

export ecom_server=sql1
export ecom_database=Ecomerce
export ecom_user=
export ecom_pass=

export hadoop_host=node01
export hadoop_port=9870
export hadoop_user=hdfs

export hdfs_host=$hadoop_host
export hdfs_port=9001

export hive_host=$hadoop_host
export hive_port=10000
export hive_user=hive
export hive_database=staging