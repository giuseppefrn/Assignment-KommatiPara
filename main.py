import argparse
import os
from pyspark.sql import SparkSession

import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger("KommatiPara-Log")
logger.setLevel(logging.DEBUG)

os.makedirs("logs", exist_ok=True)

handler = RotatingFileHandler("logs/kommatipara.log", maxBytes=200, backupCount=3)
logger.addHandler(handler)

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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--d1', type=str, required=True, help='path to the first dataset')
    parser.add_argument('--d2', type=str, required=True, help='path to the second dataset')
    parser.add_argument('--f','--list', nargs='+', help='Filter on Country', required=True)

    opt = parser.parse_args()

    logger.debug(f"Arguments parsed: {opt.d1}, {opt.d2}, {opt.f}")

    sc = SparkSession.builder.master("local").appName("KommatiPara").getOrCreate()

    df_clients = load_csv(opt.d1)
    df_fin = load_csv(opt.d2)

    df_full_clients = df_clients.join(df_fin, on=['id'], how='inner')

    df_full_clients = remove_personal_info(df=df_full_clients, personal_info=['first_name', 'last_name', 'cc_n'])

    df_full_clients = rename_columns(df=df_full_clients, columns_to_rename=[('id', 'client_identifier'), ('btc_a', 'bitcoin_address'), ('cc_t', 'credit_card_type')])
