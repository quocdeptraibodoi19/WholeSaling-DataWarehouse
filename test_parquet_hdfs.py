import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

creds = {
    "host": os.getenv("hadoop_host"),
    "port": os.getenv("hadoop_port"),
    "user": os.getenv("hadoop_user"),
}

hdfs = fs.HadoopFileSystem(host=creds.get("host"), port=9000, user=creds.get("user"),)
str_path = '/staging/delta_load/Ecomerce/User/extract_date=2024-05-01/delta_ingested_data_0.parquet'

data = [{'a': 1, 'b': 2, 'c': 3}, {'a': 10, 'b': 20, 'c': 30}]

df = pd.DataFrame(data)
table = pa.Table.from_pandas(df)

conn = pa.hdfs.connect(host=creds.get("host"), port=9000, user=creds.get("user"),)

with conn.open('/test/quok.parquet', 'wb') as writer:
    pq.write_table(table, writer)