 # KommatiPara 
  The application performs a simple ETL pipeline on two financial datasets.
  One dataset consists of information about clients, while the second one contains information about their financial details.
  It extracts data from two CSV files, transforms and merges the datasets, and finally loads the results in a new CSV file. All the steps are performed using Pyspark. 
 
 During the pipeline, some logs will be produced to help with debugging. 
 
 ## Arguments
 The application expects three arguments as input:
 - d1: the pathname for the first dataset
 - d2: the pathname for the second dataset
 - f: a list of filters to apply on D1

## Usage 
To execute the `main.py` run:
```
python3.7 main.py --d1 inputs/dataset_one.csv --d2 inputs/dataset_two.csv --f "United Kingdom" "Netherlands"
```

## Details
In detail, the application:
- Reads the two CSV files and creates pySpark Dataframe for each of them;
- Filters the dataset D1 in order to keep only clients from the Country listed in `f`;
- Joins the two datasets in a single one;
- Removes unuseful personal information from the datasets;
- Renames the columns id, btc_a, cc_t in client_identifier, bitcoin_address and credit_card_type;
- Saves the result in the client_data folder as a CSV file.

## Test spark functions
To test `filter_data` and `rename_columns` functions you can execute `spark_test.py`, i.e.:

```
python3.7 spark_test.py
```

## Tool used
- Pyspark: to perform all the transformations;
- logging: to handle the logs;
- argparse: to parse the arguments;
- chispa: to test Pyspark functions.