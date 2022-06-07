import logging
import os
import tweepy
import pandas as pd
from azure.storage.blob import BlobClient,BlobServiceClient
import azure.functions as func

def getting_tweets(name: str):
    
    # Fetching all the Twitter API & Blob Stoarge Keys
    api_key = os.environ['tweet_api_key']
    api_key_secret = os.environ['tweet_api_key_secret']
    access_token = os.environ['tweet_access_token']
    access_token_secret = os.environ['tweet_access_token_secret']
    azure_storage_conn = os.environ['sa_conn_strng']
    storage_container_name = "tweetdata"
    
    logging.info('Authenticating Twitter API')
    auth = tweepy.OAuthHandler(api_key,api_key_secret)
    auth.set_access_token(access_token,access_token_secret)
    api = tweepy.API(auth)
    
    logging.info("Getting user's public tweets")
    public_tweets = api.user_timeline(screen_name=name)
    
    # Create dataframe
    columns = ['Tweet']
    data = []
    for tweet in public_tweets:
        data.append([tweet.text])
    df = pd.DataFrame(data, columns=columns)
    
    logging.info('Storing dataframe as csv format buffer data')
    output = df.to_csv(encoding = "utf-8", index=False)
    
    # Connecting to Azure Storage and uploading CSV File to Blob Storage
    logging.info(f'Connecting to Azure storage account')
    blob = BlobClient.from_connection_string(
        conn_str=azure_storage_conn,
        container_name=storage_container_name,
        blob_name="tweets.csv"
    )

    logging.info('Attempting to store in blob storage')
    if blob.exists():
        logging.info('Blob already exist! Attempting to delete existing blob in storage')
        blob.delete_blob(delete_snapshots="include")

    logging.info('Uploading tweets.csv to blob storage')
    blob.upload_blob(output)
    
    
    
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Fetching Tweets HTTP Trigger Function Triggered.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            logging.warn("No name recieved.")
            pass
        else:
            name = req_body.get('name')

    if name:
        logging.info('Name is recieved.')
        getting_tweets(name)
        return func.HttpResponse(f"Twitter data of {name} is recieved and executed successfully.")
    else:
        return func.HttpResponse(
             f"This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
