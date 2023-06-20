from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from main import filter_data, rename_columns
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def test_remove_countries():
    source_data = [(1, 'Netherlands'),(2, 'Italy'),
             (3, 'Italy'),(4, 'United Kingdom'),
             (5, 'Netherlands'),(6, 'France'),
             (7, 'France'),(8, 'Germany')]
    
    source_df = sc.createDataFrame(source_data,  ["id", "country"])

    expected_data = [(1, 'Netherlands'), (4, 'United Kingdom'),
             (5, 'Netherlands')]
    
    filtered_df = filter_data(source_df, filters=["United Kingdom","Netherlands"], colname='country')
    expected_df = sc.createDataFrame(expected_data,  ["id", "country"])

    assert_df_equality(filtered_df, expected_df)

if __name__ == '__main__':
    sc = SparkSession.builder.master("local").appName("KommatiPara-test").getOrCreate()
    test_remove_countries()