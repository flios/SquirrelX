from get_data import get_bitcoin_price_from_tweetwise, get_tweet_from_tweetwise
from sentiment import get_sentiment_from_tweets, get_sentiment_from_file
import pandas as pd
import numpy as np
import csv

def get_features_labels(data_df, price_df, feature_type = None):

    if feature_type == 'histogram':
        get_feature = get_hist_features
    elif feature_type == 'single value':
        get_feature = get_single_value_features
    elif feature_type == 'average count':
        get_feature = get_average_count_features
    else:
        get_feature = get_hist_features

    sorted_price_df = price_df.sort_values('time')
    feature_list = []
    label_list =[]
    for i in range(len(sorted_price_df)-1):
        start_time = sorted_price_df.iloc[i,sorted_price_df.columns.get_loc('time')]
        end_time = sorted_price_df.iloc[i + 1,sorted_price_df.columns.get_loc('time')]
        single_time_interval_tweets_df = data_df[(data_df['create at']>=start_time) & (data_df['create at']<end_time)]
        single_feature = get_feature(single_time_interval_tweets_df)
        feature_list.append(list(single_feature))
        label_list.append(1 if float(sorted_price_df.iloc[i + 1,sorted_price_df.columns.get_loc('change')]) > 0 else 0)
        if i%100 == 0:
            print i
    return feature_list, label_list



def get_hist_features(data_df):
    hist_count, hist_range = np.histogram(data_df['sentiment'], bins = 15, range = [-1.,1.])
    return hist_count

def get_single_value_features(data_df):
    intervel_value = data_df['sentiment'].sum()
    return [intervel_value]

def get_average_count_features(data_df):
    feature = [0.]*4
    feature[0] = sum(1 for score in data_df['sentiment'] if score > 0.0 ) #number of the positive sentiment score
    feature[1] = sum(1 for score in data_df['sentiment'] if score < 0.0 ) #number of the negative sentiment score
    feature[2] = sum(score for score in data_df['sentiment'] if score > 0.0 )/ feature[0] #average of the positive sentiment score
    feature[3] = sum(score for score in data_df['sentiment'] if score < 0.0 )/ feature[1] #average of the negative sentiment score
    return feature

def save_list_to_file(save_list, output_file = None):
    if output_file == None:
        output_file = 'Features/temp_list'
    with open(output_file, 'w') as ofile:
        writer = csv.writer(ofile)
        writer.writerows(save_list)
        # if (type(save_list[0]) is list):
        #     writer.writerows(save_list)
        # else:
        #     writer.writerow(save_list)

def load_list_from_file(input_file = None, type_func = int):
    if input_file == None:
        input_file = 'Features/temp_list'
    feature_list = []
    with open(input_file, 'r') as ifile:
        reader = csv.reader(ifile)
        for row in reader:
            feature_list.append(map(type_func, row))
    return feature_list



def save_features_to_file(feature_list, output_file = None):
    if output_file == None:
        output_file = 'Features/features'
    save_list_to_file(feature_list, output_file)

def save_label_to_file(label_list, output_file = None):
    if output_file == None:
        output_file = 'Features/label'
    save_list_to_file(label_list, output_file)


def load_features_from_file(input_file=None, type_func=float):
    if input_file == None:
        input_file = 'Features/features'
    return load_list_from_file(input_file,type_func)

def load_label_from_file(input_file=None, type_func = int):
    if input_file == None:
        input_file = 'Features/label'
    return load_list_from_file(input_file,type_func)[0]


if __name__ == '__main__':
    # tweets_df = get_tweet_from_tweetwise()
    senti_tweet_df = get_sentiment_from_file()
    price_df = get_bitcoin_price_from_tweetwise()
    test_feature, test_label = get_features_labels(senti_tweet_df[0:1000], price_df,'single value')
    # print test_feature
    # print test_label.count(1), len(test_label)
    save_features_to_file(test_feature,'tmp_feature')
    # save_label_to_file(test_label)

    test_feature_load = load_features_from_file('tmp_feature', type_func=float)
    test_label_load = load_label_from_file()
    print cmp(test_feature_load,test_feature)
    print cmp(test_label_load, test_label)