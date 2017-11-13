from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
import pandas as pd
import get_data

def get_sentiment_for_tweets(tweets_df):

    sia = SIA()
    tw_sentiment = pd.Series(index = tweets_df.index)
    for idx in tweets_df.index:
        tw_sentiment[idx] = sia.polarity_scores(tweets_df.loc[idx,'tweets'])['compound']
    # tweets_df.loc[:,'sentiment'] = tw_sentiment
    tweets_df = tweets_df.assign(sentiment = tw_sentiment)

    tweets_sentiment_df = tweets_df.reindex(columns=['create at','sentiment'])
    return tweets_sentiment_df


if __name__ == '__main__':
    tweets_df = get_data.get_tweet_from_tweetwise()
    test = get_sentiment_for_tweets(tweets_df[0:10])
    print test



