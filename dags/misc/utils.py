#
# Utils functions
#
import logging
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd


def format_to_parquet(src_path: str, dest_path: str):
    if not src_path.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_path)
    pq.write_table(table, dest_path.replace('.csv', '.parquet'))


def format_place_to_parquet(src_path: str, dest_path: str):
    """
    Formats Place's csv file into parquet.
    Fixes null data issues in Spark and filters out empty 'Crime ID' rows

    Parameters
    ----------
    src_path : str
        Csv source file path

    dest_path : str
        Parquet destination file path
    
    """
    if not src_path.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return

    str_cols = ['Crime ID', 'Month', 'Reported by', 'Crime type', 'Last outcome category']
    double_cols = ['Longitude', 'Latitude']

    df = pd.read_csv(src_path, dtype={'Last outcome category': str, 'Crime type': str, 'Crime ID': str},
                               usecols=str_cols+double_cols)
    
    # filter out curruped Crime ID, beacuse it doesn't make sense to keep them Null
    df = df[pd.notnull(df['Crime ID'])]
    if len(df) > 1:
        # in order to fix null data issues in Spark, we have to init empty data!
        df[str_cols] = df[str_cols].fillna('null')
        df[double_cols] = df[double_cols].fillna(-0.0)
        df.to_parquet(dest_path.replace('.csv', '.parquet'))

def format_outcome_to_parquet(src_path: str, dest_path: str):
    """
    Formats Outcome's csv file into parquet.
    Fixes null data issues in Spark and filters out empty 'Crime ID' rows

    Parameters
    ----------
    src_path : str
        Csv source file path

    dest_path : str
        Parquet destination file path
    
    """
    if not src_path.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    
    str_cols = ['Crime ID', 'Outcome type']
    df = pd.read_csv(src_path, dtype={'Outcome type': str},
                               usecols=str_cols)
    
    # filter out curruped Crime ID, beacuse it doesn't make sense to keep them Null
    df = df[pd.notnull(df['Crime ID'])]
    if len(df) > 1:
        # in order to fix null data issues in Spark, we have to init empty data!
        df[str_cols] = df[str_cols].fillna('null')
        df.to_parquet(dest_path.replace('.csv', '.parquet'))