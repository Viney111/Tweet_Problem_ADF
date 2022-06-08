import logging

import azure.functions as func
from azure.storage.blob import BlobClient
import re
import nltk
import pickle
import os
import pandas as pd

model_path = "Model/logistic_model.pkl"
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')
with open(model_path, 'rb') as file:
    bow_obj = pickle.load(file)
    model = pickle.load(file)
logging.info("Model Imported")

def tokenization(data):
    """
    :param data: It will receive the tweet and perform tokenization and remove the stopwords
    :return: It will return the cleaned data
    """
    stop_words = set(nltk.corpus.stopwords.words('english'))
    stop_words.remove('no')
    stop_words.remove('not')

    tokenizer = nltk.tokenize.TweetTokenizer()

    document = []
    for text in data:
        collection = []
        tokens = tokenizer.tokenize(text)
        for token in tokens:
            if token not in stop_words:
                if '#' in token:
                    collection.append(token)
                else:
                    collection.append(re.sub("@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+", " ", token))
        document.append(" ".join(collection))
    return document

def lemmatization(data):
    """
    :param data: Receive the tokenized data
    :return: Return the cleaned data
    """
    lemma_function = nltk.stem.wordnet.WordNetLemmatizer()
    sentence = []
    for text in data:
        document = []
        words = text.split(' ')
        for word in words:
            document.append(lemma_function.lemmatize(word))
        sentence.append(" ".join(document))
    return sentence

def get_tweet_sentiment(my_tweet) -> str:
    """
        Here we'll perform predictions on the data given by the tweeter.
    """
    tokenized_data = tokenization([my_tweet])
    lemmatized_data = lemmatization(tokenized_data)
    temp = bow_obj.transform(lemmatized_data)
    pred = model.predict(temp)
    if pred == 0:
        return 'Positive'
    else:
        return 'Negative'


def prediction_main():
    """
        Description: This function will get the sentiments and stored it in CSV File in Blob Storage.
        Parameters: None
        Return: Return Nothing, Just append the sentiment message with tweet
    """
    azure_storage_conn = os.getenv('sa_conn_strng')
    storage_container_name = "tweetdata"
    azure_blob_url = os.getenv('sa_blob_url')
    
    logging.info("Reading tweets csv file from Blob Storage")
    
    # Converting CSV File to Dataframe
    df = pd.read_csv(azure_blob_url)
    
    # Predicting Sentiments of Tweets
    sentiment_results = []
    for tweet in df['Tweet']:
        sentiment_results.append(get_tweet_sentiment(tweet))
    df ['Sentiments_Predictions'] = sentiment_results
    
    output_data = df.to_csv(encoding="utf-8", index=False)
    
    logging.info("Storing the prediction sentiments file in Blob Storage")
    predict_sentiment_blob = BlobClient.from_connection_string(
        conn_str=azure_storage_conn,
        container_name=storage_container_name,
        blob_name="tweets_sentiment.csv"
    )
    if predict_sentiment_blob.exists():
        logging.info('Blob already exist! Attempting to delete existing blob in storage')
        predict_sentiment_blob.delete_blob(delete_snapshots="include")

    logging.info('Uploading tweet_sentiment.csv to blob storage')
    predict_sentiment_blob.upload_blob(output_data)

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    logging.info('Sentiments_Prediction function processed a request.')
    try:
        prediction_main()
    except Exception as ex:
        return ex
    else:
        return func.HttpResponse("Twitter Sentiments are analyzed successfully")