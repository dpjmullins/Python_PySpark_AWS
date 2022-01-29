"""
Coding Assignment

Mock data tables:
- Electricity Consumption
- Meter Master (Manually generated)
- Monthly Usage
- Transactions
- Accounts (Manually generated)
- Customers (Manually generated)
"""


import pandas as pd
import random


# Consumption table
## Generate electricity consumptions for 4 households for 3 months 01-Jan-2021 -> 31-Mar-2021
meterid = ["E101", "E201", "E301", "E401"]

## Using date_range function generate a list of dates per day in a range
dates = pd.date_range(start='2021-01-01', end='2021-03-31', freq='D')

hours_per_day = [i for i in range(0,24,1)]

consumption_list = []
for meter1 in meterid:
    for day1 in dates:
        for hour1 in hours_per_day:
            kwh = random.uniform(0.25, 0.75)
            dict1 = {
                "MeterID": meter1,
                "Date": day1,
                "Hour": hour1,
                "Usage": kwh
            }
            consumption_list.append(dict1)

df = pd.DataFrame(consumption_list)
df.to_csv("C:\\Innowatts Coding Assignment\\Mock dataset\\Consumption.csv")


# Create Monthly Usage table

## Aggregate by month for bills
month = pd.DatetimeIndex(df["Date"]).month
df = df.assign(Month = month)
monthly_totals = df.groupby(by=["MeterID", "Month"])["Usage"].sum()
monthly_totals.df = monthly_totals.to_frame()
monthly_totals.df.to_csv("C:\\Innowatts Coding Assignment\\Mock dataset\\Monthly_Usage.csv")