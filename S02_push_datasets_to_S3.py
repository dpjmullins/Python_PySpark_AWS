import boto3

s3 = boto3.resource('s3')

## Read in the datasets and store in s3
s3.Object('innowatts-123', 'Accounts.csv').put(Body=open('Mock dataset/Accounts.csv', 'rb'))
s3.Object('innowatts-123', 'Consumption.csv').put(Body=open('Mock dataset/Consumption.csv', 'rb'))
s3.Object('innowatts-123', 'Customers.csv').put(Body=open('Mock dataset/Customers.csv', 'rb'))
s3.Object('innowatts-123', 'MeterMaster.csv').put(Body=open('Mock dataset/MeterMaster.csv', 'rb'))
s3.Object('innowatts-123', 'Monthly_Usage.csv').put(Body=open('Mock dataset/Monthly_Usage.csv', 'rb'))
s3.Object('innowatts-123', 'Transactions.csv').put(Body=open('Mock dataset/Transactions.csv', 'rb'))
