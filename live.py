import numpy as np
import pandas as pd
import datetime
import stream_listener
import queue
import tweepy
import time
import sklearn
from sklearn.externals import joblib
import sentiment
import features
import json
from  get_data import get_bitcoin_price_from_dict, get_bitcoin_price_from_coindesk
import requests


class TweetDataProcess:
    def __init__(self):
        self.tweet_df = pd.DataFrame()

    def get_data_from_stream_queue(self, buff_queue):
        num = buff_queue.qsize()
        for i in range(num):
            self.tweet_df.append(pd.read_json(buff_queue.get()).loc['created_at', ['created_at', 'text']])


if __name__ == '__main__':
    import twitter_setup
    buff_que = queue.Queue()
    clf = joblib.load('rf_average_count_realtime_256_7229.pkl')
    my_api = twitter_setup.twitter_setup()
    my_listener = stream_listener.SquirrelStreamListener(buff_que)
    my_stream = tweepy.Stream(auth=my_api.auth, listener=my_listener)
    my_stream.filter(track=['bitcoin', 'btc'], languages=['en'], async=True)
    # my_listener.data_writer.running(100)
    clint = requests.session()
    tweet_num = 200
    btc_price = 0.
    last_btc_price, btw_timestamp = get_bitcoin_price_from_dict(clint.get('https://api.coindesk.com/v1/bpi/currentprice.json').json())
    while True:
        time.sleep(1)
        if buff_que.qsize() > tweet_num:
            # tweet_buff_size = buff_que.qsize()
            key_list = ['created_at', 'text', 'tweets']
            buff_dict = {k: [] for k in key_list}
            for i in range(tweet_num):
                raw_data = buff_que.get()
                data_dict = json.loads(raw_data)
                for k in (set(data_dict) & set(key_list)):
                    buff_dict[k].append(data_dict[k])
            tweet_df = pd.DataFrame({k: buff_dict[k] for k in buff_dict if len(buff_dict[k]) > 0})
            sentiment_df = sentiment.get_sentiment_from_tweets(tweet_df)
            # feature_list = []
            single_feature = features.get_average_count_features(sentiment_df)
            # feature_list.append(list(single_feature))

            btc_price, btw_timestamp = get_bitcoin_price_from_dict(clint.get('https://api.coindesk.com/v1/bpi/currentprice.json').json())

            print(btw_timestamp, end=' ')
            print('Nowcasting result:', 'up  'if(clf.predict([single_feature]) > 0) else 'down', end=' ')
            print('            Actual price movement: ', end=' ')
            if btc_price - last_btc_price > 0:
                print('+',end=' ')


            print(btc_price - last_btc_price)
            last_btc_price = btc_price

    my_stream.disconnect()
    # my_listener.data_writer.stop()
    # print test_que.qsize()
