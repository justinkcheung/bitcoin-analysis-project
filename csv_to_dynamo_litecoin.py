# Script to write csv records into dynamo db table.

from __future__ import print_function # Python 2/3 compatibility
from __future__ import division #Python 2/3 compatiblity for integer division
import argparse
import boto3
import csv
import time
from random import randint
import datetime
import json
import numpy as np
import pandas as pd
from decimal import *

AWS_KEY=""
AWS_SECRET=""
REGION="us-east-1"

dynamodb = boto3.resource('dynamodb', aws_access_key_id=AWS_KEY,
                            aws_secret_access_key=AWS_SECRET,
                            region_name=REGION)

table = dynamodb.Table('LitecoinPrices')

print('Reading CSVs')
litecoin_prices = pd.read_csv('/mnt/c/Users/justi_000/Desktop/GaTech/Personal-Projects/bitcoin-project/ltc-usd.csv', error_bad_lines=False)
print('1')

litecoin_prices.drop_duplicates(subset=['Date'], inplace=True)

# Alter Date entries to read Y-M-D for dropping extraneous entries

litecoin_prices.Date = pd.to_datetime(litecoin_prices.Date)


# Drop extraneous entries
litecoin_prices.drop(litecoin_prices[litecoin_prices['Date'] < '2017-05-01'].index, inplace=True)
litecoin_prices.drop(litecoin_prices[litecoin_prices['Date'] > '2017-05-31'].index, inplace=True)
# litecoin_prices.drop(litecoin_prices[litecoin_prices['Date'] > '2018'].index, inplace=True)

print('Dropped Additional Entries')

litecoin_prices = litecoin_prices.drop(columns=['market_cap','total_volume'])

print('Dropped Additional Columns')

# Create Pivot Table
litecoin_prices.rename(columns={'Close Price':'LitecoinPrices'}, inplace=True)
# print(litecoin_prices)
litecoin_prices.index = pd.DatetimeIndex(litecoin_prices.Date)
litecoin_prices_PT = litecoin_prices.pivot_table(values = 'LitecoinPrices', aggfunc = np.sum, index = litecoin_prices.index.date, fill_value = 0)
litecoin_prices_PT.index = pd.DatetimeIndex(litecoin_prices_PT.index)

# print(litecoin_prices_PT)

litecoin_prices_PT.index = litecoin_prices_PT.index.strftime(date_format="%m/%d/%y")


# write records to dynamo db

count = 0

litecoin_prices_dict = litecoin_prices_PT.to_dict(orient='index')
for index in litecoin_prices_dict:
    count += 1
    if count%200==0:
        print(count)
    item = {'Date': index}
    litecoin_prices_dict[index]['LitecoinPrices'] = Decimal('%.2f'%litecoin_prices_dict[index]['LitecoinPrices'])
    item.update(litecoin_prices_dict[index])
    table.put_item(Item = item)

    time.sleep(0.01) # to accomodate max write provisioned capacity for table
