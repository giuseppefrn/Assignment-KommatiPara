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