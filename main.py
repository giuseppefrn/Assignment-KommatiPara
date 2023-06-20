import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import logging
from logging.handlers import RotatingFileHandler

from typing import TypeVar, Sequence

T = TypeVar('T')
C = TypeVar('C')
D = TypeVar('D')

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

    df_clients = filter_data(df=df_clients, filters=opt.f, colname='country')