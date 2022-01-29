"""
Program using PySpark to perform JOIN / FILTER / AGGREGATION queries. 

Only 1 operation can be performed at a time.

A filtered or joined table can be first written to S3, before a second iteration of the script can perform aggregation.
"""


# LIBRARIES
from sre_compile import isstring
import boto3
import pandas as pd
from pyspark.sql import SparkSession
import json


# SESSION SETUP
spark = SparkSession \
    .builder \
    .appName("Assignment") \
    .getOrCreate()

s3 = boto3.client('s3')


# FUNCTIONS

def check_input(var_dict):
    """
    Check that the input variables are valid. If no issues then the script will proceed as normal.
    """
    ## Check S3 file input values for bucket and input_csv_1
    try:
        s3.get_object(Bucket = var_dict["s3_bucket"], Key = var_dict["input_csv_1"])
    except:
        print("ERROR: s3_bucket or input_csv_1 input values incorrect")
        quit()

    ## Check "operation" input value
    if var_dict["operation"] in ["join", "filter", "aggregate"]:
        pass
    else:
        print("ERROR: operation input value invalid")
        quit()

    ## Check input_csv_2 if a JOIN operation is called
    if var_dict["operation"] == "join":
        try:
            s3.get_object(Bucket = var_dict["s3_bucket"], Key = var_dict["input_csv_2"])
        except:
            print("ERROR: input_csv_2 input value incorrect")
            quit()

    ## Check output file configuration
    ### check output_folder input variable
    if isinstance(var_dict["output_folder"], str):
        pass
    else:
        print("ERROR: output_folder input variable must be a string")
        quit()

    ### check output_filetype input variable
    if var_dict["output_filetype"] in ["csv", "parquet"]:
        pass
    else:
        print("ERROR: output_filetype input variable can only have values 'csv' or 'parquet'.")
        quit()

    ### check output_filename input variable
    if isinstance(var_dict["output_filename"], str):
        pass
    else:
        print("ERROR: output_filename input variable must be a string")
        quit()
    

def df_join(df1, df2, var1):
    """
    JOIN QUERY
    Function takes two PySpark data frames as input
    """
    ## First check if var1 column is common to both df1 and df2
    if var1 in df1.columns and var1 in df2.columns:
        pass
    else:
        print("ERROR: Please provide a join_var variable common to both input tables.")
        quit()

    ## perform spark join
    join_df = df1.join(df2, df1[var1] == df2[var1], 'outer')

    return(join_df)

def df_filter(df, var1, value1):
    """
    FILTER QUERY
    Function takes a PySpark data frame as input
    """
    ## First check if filter variable is in the data frame
    if var1 in df.columns:
        pass
    else:
        print("ERROR: Please provide a valid filter_var column name from the input table.")
        quit()

    ## perform spark filter
    df_filter = df.filter(df[var1] == value1)

    return(df_filter)

def df_aggregate(df, var1, var2, metric):
    """
    AGGREGATE QUERY
    Function takes a PySpark data frame as input

    var1 - grouping variable
    var2 - aggregation variable
    metric - how to aggregate
    """
    ## First check if variables are in data frame
    #print(df.dtypes) #[('MeterID', 'string'), ('Date', 'string'), ('Hour', 'bigint'), ('Usage', 'double')]
    if var1 in df.columns and var2 in df.columns:
        pass
    else:
        print("ERROR: Please provide a valid aggregate_group_var and aggregate_summary column name from the input data frame.")
        quit()

    ## perform spark aggregate
    try:
        df_agg = df.groupBy(var1).agg({var2: metric})
    except:
        print("ERROR: Please confirm that the provided aggregation metric is valid. \nTry sum, max, count or min.")
        quit()

    return(df_agg)

def df_s3_write(df, format, s3bucket, folder, filename):
    """
    S3 WRITE
    Function to write a data frame to S3 in CSV or Parquet format
    """
    df = df.toPandas()
    if format == "csv":
        csv_filename = 's3://' + s3bucket + '/' + folder + '/' + filename + '.csv'
        df.to_csv(csv_filename)
    if format == "parquet":
        parq_filename = 's3://' + s3bucket + '/' + folder + '/' + filename + '.parquet'
        df.to_parquet(parq_filename)
        

# Main code

def main():

    # Load in the externally provided JSON input values
    ## Load in variables from JSON file
    with open("script_control.json") as inFile:
        var_dict = json.load(inFile)

    ## Check the the variables are valid
    check_input(var_dict)


    # Pull the specified data frames from S3
    ## Pull first file from S3
    obj = s3.get_object(Bucket = var_dict["s3_bucket"], Key = var_dict["input_csv_1"])
    ## Use pandas to convert streaming body to data frame then convert to spark format
    df1 = spark.createDataFrame( pd.read_csv(obj['Body']) )

    ## Repeat for second data frame, if required
    if var_dict["operation"] == "join":
        obj2 = s3.get_object(Bucket = var_dict["s3_bucket"], Key = var_dict["input_csv_2"])
        df2 = spark.createDataFrame( pd.read_csv(obj2['Body']) )


    # Perform the specified query operation
    if var_dict["operation"] == "join":
        df_out = df_join(df1, df2, var_dict["join_var"])

    elif var_dict["operation"] == "filter":
        df_out = df_filter(df1, var_dict["filter_var"], var_dict["filter_var_value"])

    elif var_dict["operation"] == "aggregate":
        df_out = df_aggregate(df1, var_dict["aggregate_group_var"], var_dict["aggregate_summary_var"], var_dict["aggregate_summary_metric"])


    # Write the query result to S3 bucket
    df_s3_write(df_out, var_dict["output_filetype"], var_dict["s3_bucket"], var_dict["output_folder"], var_dict["output_filename"])


if __name__ == "__main__":
    main()
