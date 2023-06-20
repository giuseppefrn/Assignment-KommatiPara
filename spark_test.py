from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from main import filter_data, rename_columns
import os
import sys

from main import sc

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

def test_rename():
    source_data = [(1, '123','123'),(2, '542', '433'),
             (3, '532', '532')]
    
    source_df = sc.createDataFrame(source_data,  ["id", "btc_a", "cc_t"])

    
    expected_df = sc.createDataFrame(source_data,  ["client_identifier", "bitcoin_address", "credit_card_type"])

    renamed_df = rename_columns(df=source_df, columns_to_rename=[('id', 'client_identifier'), ('btc_a', 'bitcoin_address'), ('cc_t', 'credit_card_type')])

    assert_df_equality(renamed_df, expected_df)

if __name__ == '__main__':
    test_remove_countries()
    test_rename()
