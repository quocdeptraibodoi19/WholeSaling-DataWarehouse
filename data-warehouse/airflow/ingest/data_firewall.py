import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import logging
import pandas as pd

from common.system_data_hooks import HiveDataHook, HDFSDataHook
from common.helpers import ConstantsProvider
from common.ingestion_strategies import (
    DataIngester,
    HDFSLandingZoneIngestionStrategy,
    HiveStagingIntestionStrategy,
)

from datetime import datetime

from fuzzywuzzy import fuzz

def resolve_ambiguities(combined_df, ambiguous_key, ambiguous_threshold=97):
    combined_df['is_ambiguous'] = False

    for index, row in combined_df.iterrows():
        for other_index ,other_row in combined_df.drop(index).iterrows():
            if other_index[1] != index[1] and fuzz.ratio(row[ambiguous_key], other_row[ambiguous_key]) > ambiguous_threshold:
                combined_df.at[index, 'is_ambiguous'] = True
                combined_df.at[index, 'ambiguous_name'] = other_row[ambiguous_key]
                combined_df.at[index, 'mapping_id'] = other_index[0]
                combined_df.at[index, 'mapping_source'] = other_index[1]

                combined_df.at[other_index, 'is_ambiguous'] = True
                combined_df.at[other_index, 'ambiguous_name'] = row[ambiguous_key]
                combined_df.at[other_index, 'mapping_id'] = index[0]
                combined_df.at[other_index, 'mapping_source'] = index[1]

def get_resolved_ambiguous_records(hive_sys: HiveDataHook, logger: logging.Logger):
    DQ_table_schema = ConstantsProvider.get_DQ_table_schema()

    check_if_dq_table_exist = f"SHOW TABLES IN {ConstantsProvider.get_staging_DW_name()} LIKE '{ConstantsProvider.get_DQ_table()}'"
    logger.info(
        f"Checking the existence of data quality table with: {check_if_dq_table_exist}"
    )
    checking_df = hive_sys.execute(query=check_if_dq_table_exist)
    if checking_df.empty:
        logger.info(f"The table {ConstantsProvider.get_DQ_table()} is being empty...")
        return pd.DataFrame(columns=DQ_table_schema)
    resolved_ambiguous_records_query = (
        f"SELECT {','.join(DQ_table_schema)} FROM {ConstantsProvider.get_DQ_table()}"
    )
    return list(hive_sys.execute(resolved_ambiguous_records_query))[0]


def data_hive_query_generator(
    hive_tables: str, primary_key: str, ambiguous_key: str, hive_sys: HiveDataHook
):
    for hive_table in hive_tables:
        query = f"SELECT {primary_key} as id, {ambiguous_key}, '{hive_table}' as source FROM {hive_table}"
        yield hive_sys.execute(query)


def compare_and_identify_ambiguous_records(
    hive_tables: list[str], primary_key: str, ambiguous_key: str, logger: logging.Logger
):
    hive_sys = HiveDataHook()
    try:
        hive_sys.connect()

        resolved_ambiguous_df = get_resolved_ambiguous_records(hive_sys, logger)
        resolved_ids = resolved_ambiguous_df['id'].tolist()
        resolved_sources  = resolved_ambiguous_df['source'].tolist()

        combined_df = pd.concat(
            data_hive_query_generator(
                hive_tables, primary_key, ambiguous_key, hive_sys
            ),
            ignore_index=True,
        )
        combined_df.set_index(["id", "source"], inplace=True)

        combined_df = combined_df[
            ~(combined_df.index.get_level_values('id').isin(resolved_ids)) 
            & ~(combined_df.index.get_level_values('source').isin(resolved_sources))
        ]
        logger.info(f"the head of combined df: {combined_df.head()}")

        resolve_ambiguities(combined_df, ambiguous_key)
        ambiguous_records_df = combined_df[combined_df['is_ambiguous']]
        print(f"len of ambiguous df: {len(ambiguous_records_df)}")
        return ambiguous_records_df.to_json(orient='records')

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hive_sys.disconnect()

def convert_json_to_dfs(json_data: str):
    return [pd.read_json(json_data, orient='records')]

def move_ambiguous_records_to_DQ_table(logger: logging.Logger, data_collection: list[pd.DataFrame]):
    hdfs_sys = HDFSDataHook()
    hive_sys = HiveDataHook()
    try:
        hdfs_sys.connect()
        hive_sys.connect()

        logger.info(f"Moving data into HDFS...")
        hdfs_ingester = DataIngester(HDFSLandingZoneIngestionStrategy())
        hdfs_ingester.ingest(
            data_collection=data_collection,
            ingested_file_name=ConstantsProvider.get_data_firewall_file(),
            HDFS_location_dir=ConstantsProvider.HDFS_LandingZone_data_firewall_base_dir(
                datetime.now().strftime("%Y-%m-%d")
            ),
        )

        DQ_table_schema = hdfs_sys.execute(command="data_schema")(
            base_dir=ConstantsProvider.HDFS_LandingZone_data_firewall_base_dir(
                datetime.now().strftime("%Y-%m-%d")
            ),
            file_name=ConstantsProvider.get_data_firewall_file(),
        )

        hive_ingester = DataIngester(HiveStagingIntestionStrategy())
        hive_ingester.ingest(
            table_columns=DQ_table_schema,
            hive_table_name=ConstantsProvider.get_DQ_table(),
            HDFS_table_location_dir=(
                ConstantsProvider.HDFS_LandingZone_data_firewall_base_dir(
                    datetime.now().strftime("%Y-%m-%d")
                )
            ),
        )

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        hive_sys.disconnect()
        hdfs_sys.disconnect()
