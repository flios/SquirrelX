from get_data import get_bitcoin_price_from_tweetwise, get_tweet_from_tweetwise
from sentiment import get_sentiment_from_tweets, get_sentiment_from_file
import pandas as pd
import numpy as np
import csv

def get_time_hist_features(tweets_sentiment_df, price_df):
    sorted_price_df = price_df.sort_values('time')
    feature_list = []
    label_list =[]
    for i in range(len(sorted_price_df)-1):
        start_time = sorted_price_df.iloc[i,sorted_price_df.columns.get_loc('time')]
        end_time = sorted_price_df.iloc[i + 1,sorted_price_df.columns.get_loc('time')]
        single_time_intervel_tweets_df = tweets_sentiment_df[(tweets_sentiment_df['create at']>=start_time) & (tweets_sentiment_df['create at']<end_time)]
        hist_count, hist_range = np.histogram(single_time_intervel_tweets_df['sentiment'], bins = 15, range = [-1.,1.])
        # print start_time, end_time
        # print len(single_time_intervel_tweets_df)
        # print hist_count.sum()
        # print hist_count
        feature_list.append(list(hist_count))
        label_list.append(1 if float(sorted_price_df.iloc[i + 1,sorted_price_df.columns.get_loc('change')]) > 0 else 0)
        if i%100 == 0:
            print i

    return feature_list, label_list

def save_features_to_file(feature_list, output_file = None):
    if output_file == None:
        output_file = 'Data/features'
    with open(output_file, 'w') as ofile:
        writer = csv.writer(ofile)
        writer.writerows(feature_list)

def save_label_to_file(label_list, output_file = None):
    if output_file == None:
        output_file = 'Data/label'
    with open(output_file, 'w') as ofile:
        writer = csv.writer(ofile)
        writer.writerow(label_list)

def read_features_from_file(input_file=None):
    if input_file == None:
        input_file = 'Data/features'
    feature_list = []
    with open(input_file, 'r') as ifile:
        reader = csv.reader(ifile)
        for row in reader:
            feature_list.append(map(int, row))
    if len(feature_list) == 1:
        feature_list = feature_list[0]
    return feature_list

def read_label_from_file(input_file=None):
    if input_file == None:
        input_file = 'Data/label'
    label_list = []
    with open(input_file, 'r') as ifile:
        reader = csv.reader(ifile)
        for row in reader:
            label_list.append(map(int, row))
    if len(label_list) == 1:
        label_list = label_list[0]
    return label_list


if __name__ == '__main__':
    tweets_df = get_tweet_from_tweetwise()
    senti_tweet_df = get_sentiment_from_file()
    price_df = get_bitcoin_price_from_tweetwise()
    test_feature, test_label = get_time_hist_features(senti_tweet_df, price_df)
    # print test_feature
    # print test_label.count(1), len(test_label)
    save_features_to_file(test_feature)
    save_label_to_file(test_label)

    test_feature_load = read_features_from_file()
    test_label_load = read_label_from_file()
    print cmp(test_feature_load,test_feature)
    print cmp(test_label_load, test_label)