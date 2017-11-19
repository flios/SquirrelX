from get_data import get_tweet_from_jsons, get_tweet_from_tweetwise
from sentiment import get_sentiment_from_tweets, get_sentiment_from_file
from features import get_average_count_features
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
from sklearn.model_selection import cross_val_score
from sklearn.metrics import precision_score
import os
import datetime


def find_close_index(target, data_list):
    low_point = 0
    high_point =len(data_list)-1
    while high_point - 1 > low_point:
        idx = (low_point + high_point) // 2
        if data_list[idx] > target:
            high_point = idx
        elif data_list[idx] < target:
            low_point = idx
        else:
            return idx-1, idx +1
    return low_point,high_point


# if __name__ == '__main__':
#     test = list(range(15))
#     print(find_close_index(6,test))


if __name__ == '__main__':
    price_df = pd.read_csv('Data/coindesk-bpi-USD-close_data-2017-11-14_2017-11-17.csv')
    for idx in price_df.index:
        price_df.loc[idx,'time'] = datetime.datetime.strptime(price_df.loc[idx,'Date'],'%Y-%m-%d %H:%M:%S')
    features_list = []
    labels_list = []
    price_time_list = list(price_df['time'])

    tweets_files_list = []
    tweets_json_list = []
    for root, dirs, files in os.walk("Data"):
        for name in files:
            if name.endswith('.json'):
                tweets_files_list.append(os.path.join(root,name))

    for i in range(len(tweets_files_list)):
        with open(tweets_files_list[i]) as ifile:
            json_list = list(ifile)
            if i % 2 == 1:
                continue
            tweet_df = get_tweet_from_jsons(json_list)
            sentiment_df = get_sentiment_from_tweets(tweet_df, save_result=True,
                                                     output_file=os.path.join('Features',(tweets_files_list[i].split('\\')[1]).split('.')[0] + '.sen'))
            single_feature =get_average_count_features(sentiment_df)
            features_list.append(single_feature)
            price_idx_0 = find_close_index(tweet_df['time'][0], price_time_list)
            price_idx_1 = find_close_index(tweet_df['time'][len(tweet_df) - 1], price_time_list)
            labels_list.append(1 if (price_df['Close Price'][price_idx_1[1]-1] > price_df['Close Price'][price_idx_0[1]-1]) else 0)
            # tweets_json_list += list(ifile)
    # tweets_df = get_tweet_from_jsons(tweets_json_list)
    train_ratio = 0.7
    train_num = int(len(labels_list) * 0.7)
    train_set_feature_list, train_set_label_list = features_list[:train_num], labels_list[:train_num]
    test_set_feature_list, test_set_label_list = features_list[train_num:], labels_list[train_num:]
    if True:
        print('start to training, ',end=' ')
        print(train_num)
        clf = RandomForestClassifier(n_estimators=256)
        clf = clf.fit(train_set_feature_list, train_set_label_list)
        joblib.dump(clf, 'rf_average_count_realtime.pkl')
    else:
        clf = joblib.load('rf_average_count_realtime.pkl')

    result = clf.predict(test_set_feature_list)
    result_arr = np.array(result)
    test_set_arr = np.array(test_set_label_list)
    test_result = np.logical_xor(result_arr, test_set_arr)
    print(precision_score(test_set_label_list, result, average=None))