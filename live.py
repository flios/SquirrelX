import numpy as np
import pandas as pd
import datetime
import stream_listener
import Queue
import tweepy
import time
import sklearn
from sklearn.externals import joblib
import sentiment
import features
import json
class TweetDataProcess:
    def __init__(self):
        self.tweet_df = pd.DataFrame()

    def get_data_from_stream_queue(self, buff_queue):
        num = buff_queue.qsize()
        for i in range(num):
            self.tweet_df.append(pd.read_json(buff_queue.get()).loc['created_at', ['created_at', 'text']])



if __name__ == '__main__':
    import twitter_setup
    buff_que = Queue.Queue()
    clf = joblib.load('rf.pkl')
    my_api = twitter_setup.twitter_setup()
    my_listener = stream_listener.SquirrelStreamListener(buff_que)
    my_stream = tweepy.Stream(auth=my_api.auth, listener=my_listener)
    my_stream.filter(track=['bitcoin', 'btc'], languages=['en'], async=True)
    # my_listener.data_writer.running(100)
    while True:
        if buff_que.qsize() > 20:
            tweet_num = buff_que.qsize()
            key_list = ['created_at', 'text', 'tweets']
            buff_dict = {k: [] for k in key_list}
            for i in range(tweet_num):
                raw_data = buff_que.get()
                data_dict = json.loads(raw_data)
                for k in (set(data_dict) & set(key_list)):
                    buff_dict[k].append(data_dict[k])
            tweet_df = pd.DataFrame({k: buff_dict[k] for k in buff_dict if len(buff_dict[k]) > 0})
            sentiment_df = sentiment.get_sentiment_from_tweets(tweet_df)
            feature_list = []
            single_feature = features.get_hist_features(sentiment_df)
            feature_list.append(list(single_feature))
            print clf.predict(feature_list)
            time.sleep(10)

    my_stream.disconnect()
    # my_listener.data_writer.stop()
    # print test_que.qsize()
