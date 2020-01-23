import constants as c
import drive_test as drive
import dataresource as collector
import database_handler as db


class DataCollection(object):
    def __init__(self):
        self.twitter_api_args = collector.twitter_auth()
        self.news_api_args = collector.news_api_auth()
        self.collected_tweets = None
        self.collected_news = None

    def collect_tweets(self, locale):
        query_set = c.QUERIES
        query_filter = c.TWITTER_PARAMS["GENERIC_QUERY_FILTER"]
        locale = locale.strip().lower()
        if locale == "en-it":
            query_filter = c.TWITTER_PARAMS["IT_EN_QUERY_FILTER"]
        elif locale == "it-it":
            query_set = c.QUERIES_IT
            query_filter = c.TWITTER_PARAMS["IT_EN_QUERY_FILTER"]
        else:
            return None
        tweets = collector.get_tweets(query_set=query_set,
                                      twitter_args=self.twitter_api_args,
                                      query_filter=query_filter)
        if tweets:
            processed_tweets = collector.process_tweets(tweets)
            if not processed_tweets.empty:
                processed_tweets.reset_index().drop(columns=['index'],
                                                    inplace=True)
                duplicacy_subset = list(set(processed_tweets.columns) - {
                    "created_time"})
                processed_tweets.drop_duplicates(subset=duplicacy_subset,
                                                 inplace=True)
                self.collected_tweets = processed_tweets
        return self.collected_tweets

    def collect_news(self, locale):
        news_sources = c.NEWS_SOURCES
        locale = locale.strip().lower()
        query_set = c.QUERIES
        if locale == "it-it":
            query_set = c.QUERIES_IT
        else:
            return None
        news_collection = collector.get_news(queries=query_set,
                                             sources=news_sources,
                                             news_api=self.news_api_args)
        if not news_collection.empty():
            news_collection.reset_index().drop(columns=["index"], inplace=True)
            self.collected_news = news_collection
        return self.collected_news
