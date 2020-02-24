#!usr/bin/env python3

# PS: To run this from outside the module python3 -m src.kafkastreams.index1


from dotenv import load_dotenv
from datetime import datetime
from kafka import KafkaConsumer
import logging
import faust
import asyncio
import json
# from ..elastic_search import elasticsearch_consumer
# from ..elastic_search import elastic_twitter_producer
# from ..elastic_search import TwitterProducer
# from ..elastic_search.elastic_twitter_producer import TwitterProducer
# from ..elastic_search.elastic_twitter_producer import TwitterProducer


class StreamFilterTweets(faust.Record):
    text: str
    followers: int


app = faust.App('myapp', broker='kafka://localhost')
topic = app.topic('twitter_topics', key_type='bytes', partitions=3)


@app.agent(value_type=StreamFilterTweets,)
async def confirm_followers(tweets, serializer='json'):
    print('NOW INSIDE AGENT!!', app.topic)
    async for tweet in tweets:
        print('this is the tweet, ladies and gentlemen>>>>>>>>>>', tweet)
        the_tweet = json.loads(tweet)
        if the_tweet['followers'] > 2000:
            print('WE FOUND SOMEONE!!!!!!!')


if __name__ == '__main__':
    print('abput to start it all')
    app.main()
