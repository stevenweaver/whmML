import requests
import datetime
from pymongo import MongoClient
import json
import itertools
import csv


with open('../config.json') as config_h:
    config = json.load(config_h)
    symbols = config['symbols']
    db = config['db']

with open('./csvs/coinbaseUSD_1-min_data_2014-12-01_to_2017-05-31.csv') as coinbase_fh:
    reader = csv.DictReader(coinbase_fh)
    tickers = [ticker for ticker in reader]

threshold = 10
client = MongoClient(db['host'], db['port'])
db = client[db['name']]

collection  = 'ticker'

def format(entry):

	time = datetime.datetime.fromtimestamp(int(entry["Timestamp"]))
	name = "BTC"
 	price = float(entry["Close"])
 	lastVolume = float(entry["Volume_(BTC)"])

	entry = {
			'timestamp': time ,
			 'ticker': name,
			 'price': price,
			 'lastVolue' : lastVolume
			}

	return entry


# Split ticker into groups of threshold in order to play nice with cryptocompare server
# import pdb;pdb.set_trace()
db[collection].insert_many([format(entry) for entry in tickers])
