import pandas as pd
import numpy as np
import json
import datetime

def get_tweet_from_tweetwise(fin = None):
    if fin == None:
        fin = "Data/tweets_raw.txt"
    with open(fin) as ifile:
        tweets = ifile.readlines()
    tweets_list = []
    for i in range(len(tweets)):
        tweets_list.append(tweets[i].rstrip('||\n').split('||'))

    tweets_arr = np.array(tweets_list)
    tweets_df = pd.DataFrame(tweets_arr, columns=['author','tweets','create at'])
    return tweets_df

def get_bitcoin_price_from_tweetwise(fin = None):
    if fin == None:
        fin = "Data/prices.txt"
    price_list = []
    with open(fin, 'r') as iflie:
        for line in iflie:
            price = json.loads(line)
            price = dict(price, **(price['ticker']))
            del price['ticker']
            price_list.append(price)
    price_df = pd.DataFrame(price_list)
    price_time = pd.Series(index = price_df.index)
    for idx in price_df.index:
        price_time.loc[idx] = datetime.datetime.utcfromtimestamp(price_df.loc[idx,'timestamp']).strftime('%Y-%m-%dT%H:%M:%S')
    price_df = price_df.assign(time = price_time)
    return price_df

def get_tweet_from_jsons(json_list):
    key_list=['created_at','text','tweets']
    buff_dict = {k:[] for k in key_list}
    for json_obj in json_list:
        data_dict = json.loads(json_obj)
        for k in (set(data_dict) & set(key_list)):
            buff_dict[k].append(data_dict[k])
    tweet_df = pd.DataFrame({k:buff_dict[k] for k in buff_dict if len(buff_dict[k]) > 0 })
    return tweet_df




if __name__ == '__main__':
    with open('Data/20171115212359.json') as ifile:
        json_list = list(ifile)
        tweet_df = get_tweet_from_jsons(json_list)
    print(tweet_df)


