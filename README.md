# Python_PySpark_AWS

Python assignment to learn how to use PySpark and AWS S3. 

It involves a spark program which retrieves CSV files from S3, performs SQL-style queries on them, then outputs them back to S3 in CSV or Parquet format.

* The program is controlled by a JSON configuration file. 
* A SQL database containing multiple tables was first-generated for this program.

## Mock dataset
There are 6 tables in the mock electricity company dataset.

1. Accounts
2. Consumption
3. Customers
4. MeterMaster
5. Transactions
6. Monthly Usage
