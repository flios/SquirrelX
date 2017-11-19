from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
import pandas as pd
from get_data import get_tweet_from_tweetwise
import timeit


def get_sentiment_from_tweets(tweets_df, save_result=False, output_file=None):

    sia = SIA()
    tw_sentiment = pd.Series(index=tweets_df.index)
    if 'text' in tweets_df:
        tweet_label = 'text'
    elif 'tweets' in tweets_df:
        tweet_label = 'tweets'
    else:
        tweet_label = 'tweet'
    for idx in tweets_df.index:
        tw_sentiment[idx] = sia.polarity_scores(tweets_df.loc[idx, tweet_label])['compound']
    # tweets_df.loc[:,'sentiment'] = tw_sentiment
    tweets_df = tweets_df.assign(sentiment=tw_sentiment)

    tweets_sentiment_df = tweets_df.reindex(columns=['time', 'sentiment'])

    if save_result:
        if output_file is None:
            output_file = 'Data/sentiment'
        tweets_sentiment_df.to_json(output_file)

    return tweets_sentiment_df


def get_sentiment_from_file(input_file=None):
    if input_file is None:
        input_file = 'Data/sentiment'
    tweets_sentiment_df = pd.read_json(input_file)
    return tweets_sentiment_df


if __name__ == '__main__':
    tweets_df = get_tweet_from_tweetwise()
    start = timeit.default_number()
    test = get_sentiment_from_tweets(tweets_df, save_result=True)
    end = timeit.default_number()
    test2 = get_sentiment_from_file()
    print(end - start)
    # print test, test2