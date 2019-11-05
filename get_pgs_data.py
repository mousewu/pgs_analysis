import os
import pandas as pd
import urllib.parse
from pymongo import MongoClient


DB_NAME = 'parity'

def initMongo(client,collection):
	db = client[DB_NAME]
	try:
		db.create_collection(collection)
	except:
		pass
	return db[collection]

if __name__ == "__main__":
    uri = 'mongodb://root:longhash123' + urllib.parse.quote('!@#') + 'QAZ@172.16.144.88/admin'
    db_client = MongoClient(uri, connect=True)
    table = initMongo(db_client, 'PGS')
    df_tx = pd.DataFrame(list(table.find()))

    df_tx = df_tx.drop_duplicates(subset='transactionHash', keep='last')
    df_tx.to_csv("pgs.csv")
