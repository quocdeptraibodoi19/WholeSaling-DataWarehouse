# import os
# import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
# from pyarrow import fs

# creds = {
#     "host": os.getenv("hadoop_host"),
#     "port": os.getenv("hadoop_port"),
#     "user": os.getenv("hadoop_user"),
# }

# hdfs = fs.HadoopFileSystem(host=creds.get("host"), port=9000, user=creds.get("user"),)
# str_path = '/staging/delta_load/Ecomerce/User/extract_date=2024-05-01/delta_ingested_data_0.parquet'

# data = [{'a': 1, 'b': 2, 'c': 3}, {'a': 10, 'b': 20, 'c': 30}]

# df = pd.DataFrame(data)
# table = pa.Table.from_pandas(df)

# conn = pa.hdfs.connect(host=creds.get("host"), port=9000, user=creds.get("user"),)

# with conn.open('/test/quok.parquet', 'wb') as writer:
#     pq.write_table(table, writer)

import pandas as pd
import disamby.preprocessors as pre
from disamby import Disamby

df = pd.DataFrame({
    'id': ['1', '2', '3'],
    'name':     ['Luca Georger',        'Luca Geroger',         'Adrian Sulzer'],
    'address':  ['Mira, 34, Augsburg',  'Miri, 34, Augsburg',   'Milano, 34']},
)
df.set_index('id', inplace=True)

pipeline = [
    pre.normalize_whitespace,
    pre.remove_punctuation,
    lambda x: pre.trigram(x) + pre.split_words(x)  # any python function is allowed
]

dis = Disamby(df, pipeline)

# let disamby compute disambiguated sets. Node that a threshold must be given or it
# defaults to 0.
dis.disambiguated_sets(threshold=0.5)
[{'L2', 'L1'}, {'O1'}]  # output

# To check if the sets are accurate you can get the rows from the
# pandas dataframe like so:
df.loc[['L2', 'L1']]