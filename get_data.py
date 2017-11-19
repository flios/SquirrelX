import pandas as pd
import numpy as np
import json
import datetime
import requests


def get_tweet_from_tweetwise(fin = None):
    if fin is None:
        fin = "Data/tweets_raw.txt"
    with open(fin) as ifile:
        tweets = ifile.readlines()
    tweets_list = []
    for i in range(len(tweets)):
        tweets_list.append(tweets[i].rstrip('||\n').split('||'))

    tweets_arr = np.array(tweets_list)
    tweets_df = pd.DataFrame(tweets_arr, columns=['author', 'tweets', 'create at'])
    return tweets_df


def get_bitcoin_price_from_tweetwise(fin = None):
    if fin is None:
        fin = "Data/prices.txt"
    price_list = []
    with open(fin, 'r') as iflie:
        for line in iflie:
            price = json.loads(line)
            price = dict(price, **(price['ticker']))
            del price['ticker']
            price_list.append(price)
    price_df = pd.DataFrame(price_list)
    price_time = pd.Series(index=price_df.index)
    for idx in price_df.index:
        price_time.loc[idx] = datetime.datetime.utcfromtimestamp(price_df.loc[idx, 'timestamp']).strftime('%Y-%m-%dT%H:%M:%S')
    price_df = price_df.assign(time=price_time)
    return price_df


def get_tweet_from_jsons(json_list):
    key_list = ['text', 'tweets', 'timestamp_ms', 'time']
    buff_dict = {k: [] for k in key_list}
    for json_obj in json_list:
        data_dict = json.loads(json_obj)
        for k in (set(data_dict) & set(key_list)):
            buff_dict[k].append(data_dict[k])
        buff_dict['time'] = [0] * len(buff_dict['timestamp_ms'])
        for i in range(len(buff_dict['timestamp_ms'])):
            buff_dict['time'][i] = datetime.datetime.utcfromtimestamp(int(buff_dict['timestamp_ms'][i])/1000)
        # del buff_dict['timestamp_ms']
    tweet_df = pd.DataFrame({k: buff_dict[k] for k in buff_dict if len(buff_dict[k]) > 0})
    return tweet_df


def get_bitcoin_price_from_coindesk():
    clint = requests.session()
    r = clint.get('https://api.coindesk.com/v1/bpi/currentprice.json')
    price_dict = r.json()
    return price_dict


def get_bitcoin_price_from_dict(dict_obj):
    price_usd = (dict_obj['bpi']['USD']['rate_float'])
    price_timestamp = datetime.datetime.strptime(dict_obj['time']['updatedISO'].split('+')[0], "%Y-%m-%dT%H:%M:%S")
    return price_usd, price_timestamp




if __name__ == '__main__':
    with open('Data/20171115212359.json') as ifile:
        json_list = list(ifile)
        tweet_df = get_tweet_from_jsons(json_list)
    print(tweet_df)
    # price_dic = get_bitcoin_price_from_coindesk()
    # print(get_bitcoin_price_from_dict(price_dic))