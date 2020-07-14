import boto3
import json
from datetime import datetime
import calendar
import random
import time
import sys
import re
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

stream_name = 'Sapienza_F' 

session = boto3.Session(
        aws_access_key_id='',
        aws_secret_access_key='',)

kinesisClient = session.client('firehose',region_name='us-east-1')

def deEmojify(text):
        regrex_pattern = re.compile(pattern = "["
            u"\U0001F600-\U0001F64F"  # emoticons
            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
            u"\U0001F680-\U0001F6FF"  # transport & map symbols
            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               "]+", flags = re.UNICODE)
        return regrex_pattern.sub(r'',text)

class TweetStreamListener(StreamListener):        
    
    def on_data(self, data):
        
        tweet = json.loads(data)
        
        if "text" in tweet.keys():
            
            payload = {'tweet': deEmojify(str(tweet['text']))},
            print(payload)

            try:
                put_response = kinesisClient.put_record(
                    DeliveryStreamName=stream_name,
                    Record={
                        'Data': json.dumps(payload)
                    })
            except (AttributeError, Exception) as e:
                print (e)
                pass
        return True
        
    # on failure
    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    
    listener = TweetStreamListener()
    
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    stream = Stream(auth, listener)
    stream.sample(languages=["en"])
