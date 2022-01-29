# Python_PySpark_AWS

Python assignment to learn how to use PySpark and AWS S3. 

It involves a spark program which retrieves CSV files from S3, performs SQL-style queries on them, then outputs them back to S3 in CSV or Parquet format.

* The program is controlled by a JSON configuration file. 
* A SQL database containing multiple tables was first-generated for this program.

## Mock dataset
There are 6 CSV tables in the mock electricity company dataset.

1. Accounts
2. Consumption
3. Customers
4. MeterMaster
5. Transactions
6. Monthly Usage

## Script Description

1. S01_generate_mock_data.py
    1. Script to generate electricity usage data
2. S02_push_datasets_to_S3.py
    1. Script to oush datasets to AWS S3
3. S03_spark_queries.py
    1. Main program which takes "script_control.json" as input and performs SQL queries on input files from AWS S3.
    2. Resulting files are output to AWS S3. 
    3. Only one query is performed at a time by the program.
