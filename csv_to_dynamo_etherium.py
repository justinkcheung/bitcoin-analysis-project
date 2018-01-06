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

table = dynamodb.Table('EtheriumPrices')

print('Reading CSVs')
etherium_prices = pd.read_csv('/mnt/c/Users/justi_000/Desktop/GaTech/Personal-Projects/bitcoin-project/coindesk-ETH-close_data-2017-04-30_2017-12-31.csv', error_bad_lines=False)
print('1')

etherium_prices.drop_duplicates(subset=['Date'], inplace=True)

etherium_prices.drop(etherium_prices[etherium_prices['Date'] < '2017-04-30'].index, inplace=True)
etherium_prices.drop(etherium_prices[etherium_prices['Date'] > '2017-05-31'].index, inplace=True)
# etherium_prices.drop(etherium_prices[etherium_prices['Date'] > '2017-12-31'].index, inplace=True)

print('Dropped Additional Entries')

etherium_prices.Date = pd.to_datetime(etherium_prices.Date, format='%Y-%m-%d %H:%M:%S')
for i in range(0, len(etherium_prices.Date)):
    etherium_prices.Date[i] = etherium_prices.Date[i] + datetime.timedelta(days=1)
print("Etherium price dates aligned:")

etherium_prices_time = etherium_prices['Date'].apply(lambda x: pd.Series(str(x).split(' ')))
etherium_prices_time.rename(columns={0:'Date',1:'Time'}, inplace = True)

etherium_prices.drop(etherium_prices[etherium_prices_time.Time < '12:00:00'].index, inplace=True)
print("Dropped Duplicates")

#Create Pivot Table
etherium_prices.rename(columns={'Close Price':'EtheriumPrices'}, inplace=True)
etherium_prices.index = pd.DatetimeIndex(etherium_prices.Date)
etherium_prices_PT = etherium_prices.pivot_table(values = 'EtheriumPrices', aggfunc = np.sum, index = etherium_prices.index.date, fill_value = 0)
etherium_prices_PT.index = pd.DatetimeIndex(etherium_prices_PT.index)

etherium_prices_PT.index = etherium_prices_PT.index.strftime(date_format="%m/%d/%y")

# write records to dynamo db

count = 0

etherium_prices_dict = etherium_prices_PT.to_dict(orient='index')
for index in etherium_prices_dict:
    count += 1
    if count%200==0:
        print(count)
    item = {'Date': index}
    etherium_prices_dict[index]['EtheriumPrices'] = Decimal('%.2f'%etherium_prices_dict[index]['EtheriumPrices'])
    item.update(etherium_prices_dict[index])
    # print(item)
    table.put_item(Item = item)

    time.sleep(0.01) # to accomodate max write provisioned capacity for table
