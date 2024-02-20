export AIRFLOW_HOME=$(pwd)/airflow

export ENV=local

export DBT_PROJECT_DIR=$(pwd)/dbt_wholesaleDW
export DBT_PROFILES_DIR=$(pwd)/dbt_wholesaleDW

export USER=$(whoami)

export hr_server=sql1
export hr_database=HumanResourceSystem
export hr_user=sa
export hr_pass=@Quoc1234

export wholesale_server=sql1
export wholesale_database=WholeSaling
export wholesale_user=sa
export wholesale_pass=@Quoc1234

export product_server=sql1
export product_database=Product
export product_user=sa
export product_pass=@Quoc1234

export ecom_server=sql1
export ecom_database=Ecomerce
export ecom_user=sa
export ecom_pass=@Quoc1234

export hadoop_host=hadoop-master
export hadoop_port=9870
export hadoop_user=hadoop

export hdfs_host=$hadoop_host
export hdfs_port=9001

export hive_host=$hadoop_host
export hive_port=10000
export hive_user=$hadoop_user
export hive_database=staging

export presto_host=$hadoop_host
export presto_port=8080
export presto_user=$hadoop_user
export presto_catalog=hive
export presto_schema=staging