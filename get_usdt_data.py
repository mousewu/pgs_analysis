import pandas as pd
from pymongo import MongoClient
import pymongo

def initMongo(client,db_name,collection):
    """
    Given a mongo client instance, create db/collection if either doesn't exist

    Parameters:
    -----------
    client <mongodb Client>

    Returns:
    --------
    <mongodb Client>
    """
    db = client[db_name]
    try:
        db.create_collection(collection)
    except:
        pass
    try:
        # Index the block number so duplicate records cannot be made
        db[collection].create_index([("blockNumber", pymongo.DESCENDING)])
    except:
        pass

    return db[collection]

if __name__ == "__main__":
    table = pd.read_csv("/Users/wuxianyue/codes/pgs_analysis/pgs_network.csv")
    addresses = table['account'].values
    print(addresses)

    url1 = "mongodb://longhashdba:longhash123QAZ@localhost/parity"
    db_client1 = MongoClient(url1, connect=True)
    table_source = initMongo(db_client1, "parity", "USDT")

    df = pd.DataFrame(list(table_source.find({"$and":[
        {"from":{"$in":addresses}},
        {"to":{"$in":addresses}}
    ]})))

    df = df.drop_duplicates(subset="transactionHash",keep="last")
    df.to_csv("usdt_tx.csv")