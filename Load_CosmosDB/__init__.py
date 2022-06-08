import logging

import azure.functions as func
import os
import pandas as pd
from pymongo import MongoClient

def loading_data_to_cosmosDB():
    """
        Description: Loading Sentiment Analyzer data into Azure CosmosDB    
        Parameters: None
        Output: Just post Successful log for loading data into CosmosDB
    """
    conn_str = os.getenv('PRIMARY_CONN_STR')
    client = MongoClient(conn_str)
    db = client['TwitterData']
    collection = db['tweet_sentiments']
    sentiment_blob_url = os.getenv('sa_sentiment_blob_url')
    
    logging.info("Reading form Blob Tweet Sentiments File")
    df = pd.read_csv(sentiment_blob_url)
    sentiment_dict = df.to_dict('records')
    
    # Deleting documents present earlier in connection, if any and then loading data to CosmosDB
    if collection.count == 0:
        collection.insert_many(sentiment_dict)
    else:
        collection.delete_many({})
        collection.insert_many(sentiment_dict)
    logging.info("Records Inserted in CosmosDB")


def main(req: func.HttpRequest) -> func.HttpResponse:
    
    logging.info('Load_CosmosDB function processed a request.')
    try:
        loading_data_to_cosmosDB()
    except Exception as ex:
        return ex
    else:
        return func.HttpResponse("Data loaded to CosmosDB Successfully")

