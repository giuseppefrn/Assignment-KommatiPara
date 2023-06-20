import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import logging
from logging.handlers import RotatingFileHandler

from typing import TypeVar, Sequence
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

T = TypeVar('T')
C = TypeVar('C')
D = TypeVar('D')

logger = logging.getLogger("KommatiPara-Log")
logger.setLevel(logging.DEBUG)

os.makedirs("logs", exist_ok=True)

handler = RotatingFileHandler("logs/kommatipara.log", maxBytes=200, backupCount=3)
logger.addHandler(handler)

sc = SparkSession.builder.master("local").appName("KommatiPara").getOrCreate()

def load_csv(filepath):
    """
    Load csv from a given filepath.

    :param str filepath: path to the .csv file
    :return: pyspark dataframe
    """
    if os.path.exists(filepath):
        df = sc.read.option("header", "true").csv(filepath)
        logger.debug(f"Data loaded: {filepath}")
        #log dataset loaded
        return df
    logger.fatal(f"File doesn't exist: {filepath}")

def filter_data(df: D, filters: Sequence[T], colname: C) -> D:
    """
    Filters data given list of filters and a column name.

    :param dataframe df: dataframe to be filtered
    :param list filters: list of countries to keep
    :param colname str colname: name of the column for the filter criteria
    :return: filtered dataset
    """
    logger.debug(f"Filtering column: {colname} values: {filters}")
    return df.filter(col(colname).isin(filters))

def remove_personal_info(df, personal_info):
    """
    Drops personal info from the dataframe
    :param dataframe df: dataframe where to remove personal info
    :param personal_info list: list of columns to drop
    :return modified dataframe
    """
    return df.drop(*personal_info)

def rename_columns(df: D, columns_to_rename: Sequence[T]) -> D:
    """
   Renames columns from dataframe.

   :param dataframe df: dataframe where to rename
   :param list columns_to_rename: list of tuples (old name, new name)
   :return: dataframe with renamed columns
   """
    logger.debug(f"Data to be renamed: {columns_to_rename}")
    for (old,new) in columns_to_rename:
        df = df.withColumnRenamed(old, new)

    return df

def save_csv_output_file(df, path):
    """
    Save output inside the path folder as csv and overwite if already exists
    :param dataframe df: dataframe to be saved
    :param string path: path to the folder where to save the output
    :return void
    """

    df.write.mode('overwrite').csv(path)
    logger.debug(f"Output saved on: {path}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--d1', type=str, required=True, help='path to the first dataset')
    parser.add_argument('--d2', type=str, required=True, help='path to the second dataset')
    parser.add_argument('--f','--list', nargs='+', help='Filter on Country', required=True)

    opt = parser.parse_args()

    logger.debug(f"Arguments parsed: {opt.d1}, {opt.d2}, {opt.f}")

    df_clients = load_csv(opt.d1)
    df_fin = load_csv(opt.d2)

    df_clients = filter_data(df=df_clients, filters=opt.f, colname='country')

    df_full_clients = df_clients.join(df_fin, on=['id'], how='inner')

    df_full_clients = remove_personal_info(df=df_full_clients, personal_info=['first_name', 'last_name', 'cc_n'])

    df_full_clients = rename_columns(df=df_full_clients, columns_to_rename=[('id', 'client_identifier'), ('btc_a', 'bitcoin_address'), ('cc_t', 'credit_card_type')])


    save_csv_output_file(df=df_full_clients, path='client_data')