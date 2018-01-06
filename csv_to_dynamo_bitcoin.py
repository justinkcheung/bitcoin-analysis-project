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

table = dynamodb.Table('BitcoinPrices')

print('Reading CSVs')
bitcoin_prices = pd.read_csv('/mnt/c/Users/justi_000/Desktop/GaTech/Personal-Projects/bitcoin-project/coindesk-bpi-USD-close_data-2017-04-30_2017-12-31.csv', error_bad_lines=False)
print('1')

bitcoin_prices.drop_duplicates(subset=['Date'], inplace=True)

bitcoin_prices.drop(bitcoin_prices[bitcoin_prices['Date'] < '2017-05-01'].index, inplace=True)
bitcoin_prices.drop(bitcoin_prices[bitcoin_prices['Date'] > '2017-05-31'].index, inplace=True)
# bitcoin_prices.drop(bitcoin_prices[bitcoin_prices['Date'] > '2018'].index, inplace=True)

print('Dropped Duplicates')

bitcoin_prices.Date = pd.to_datetime(bitcoin_prices.Date, format='%Y-%m-%d %H:%M:%S')
# for i in range(0, len(bitcoin_prices.Date)):
#     bitcoin_prices.Date[i] = bitcoin_prices.Date[i] + datetime.timedelta(days=1)
bitcoin_prices.index = pd.DatetimeIndex(bitcoin_prices.Date)

# Create Pivot Table
bitcoin_prices.rename(columns={'Close Price':'BitcoinPrices'}, inplace=True)
# print(bitcoin_prices)
bitcoin_prices.index = pd.DatetimeIndex(bitcoin_prices.Date)
bitcoin_prices_PT = bitcoin_prices.pivot_table(values = 'BitcoinPrices', aggfunc = np.sum, index = bitcoin_prices.index.date, fill_value = 0)
bitcoin_prices_PT.index = pd.DatetimeIndex(bitcoin_prices_PT.index)

# print(bitcoin_prices_PT)

bitcoin_prices_PT.index = bitcoin_prices_PT.index.strftime(date_format="%m/%d/%y")


# write records to dynamo db

count = 0

bitcoin_prices_dict = bitcoin_prices_PT.to_dict(orient='index')
for index in bitcoin_prices_dict:
    count += 1
    if count%200==0:
        print(count)
    item = {'Date': index}
    bitcoin_prices_dict[index]['BitcoinPrices'] = Decimal('%.2f'%bitcoin_prices_dict[index]['BitcoinPrices'])
    item.update(bitcoin_prices_dict[index])
    table.put_item(Item = item)

    time.sleep(0.01) # to accomodate max write provisioned capacity for table
