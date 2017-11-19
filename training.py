from get_data import get_bitcoin_price_from_tweetwise, get_tweet_from_tweetwise
from sentiment import get_sentiment_from_tweets, get_sentiment_from_file
from features import get_hist_features,load_label_from_file, load_features_from_file
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.externals import joblib
from sklearn.model_selection import cross_val_score
from sklearn.metrics import precision_score


if __name__ == '__main__':
    # tweets_df = get_tweet_from_tweetwise()
    # senti_tweet_df = get_sentiment_from_file()
    # price_df = get_bitcoin_price_from_tweetwise()
    print('start to get feature')
    # features_list, label_list = get_time_hist_features(senti_tweet_df, price_df)
    features_list = load_features_from_file('Features/average_count')
    label_list = load_label_from_file()
    train_ratio = 0.7
    train_num = int(len(label_list) * 0.7)
    train_set_feature_list, train_set_label_list = features_list[:train_num], label_list[:train_num]
    test_set_feature_list, test_set_label_list = features_list[train_num:], label_list[train_num:]
    if True:
        print('start to training, ',end=' ')
        print(train_num)
        clf = RandomForestClassifier(n_estimators=4)
        clf = clf.fit(train_set_feature_list, train_set_label_list)
        joblib.dump(clf, 'rf_average_count.pkl')
    else:
        clf = joblib.load('rf_average_count.pkl')

    result = clf.predict(test_set_feature_list)
    result_arr = np.array(result)
    test_set_arr = np.array(test_set_label_list)
    test_result = np.logical_xor(result_arr, test_set_arr)
    print(precision_score(test_set_label_list, test_result, average=None))
    # print test_result
    # print test_result.sum(), (len(test_result))
    # print 1 - float(test_result.sum())/float(len(test_result))
    # print cross_val_score(clf,train_set_feature_list, train_set_label_list).mean()

    # clf_adaboost = AdaBoostClassifier(n_estimators= 400)
    # print cross_val_score(clf_adaboost,train_set_feature_list,train_set_label_list).mean()
