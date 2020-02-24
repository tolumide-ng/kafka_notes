#!/usr/bin/env python3
from twitter import Api
from dotenv import load_dotenv
from kafka import KafkaProducer
import os
import json
import logging
load_dotenv()


CONSUMER = os.getenv('CONSUMER_KEY')
CONSUMER_SECRET = os.getenv('CONSUMER_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')


class TwitterProducer:
    logging.basicConfig(filename='tweets.py', level=logging.NOTSET)

    def run(self):
        self.stream_mentions()

    # def _producer(self):
    #     pass

    def __init__(self):
        self.api = Api(CONSUMER,
                       CONSUMER_SECRET,
                       ACCESS_TOKEN,
                       ACCESS_TOKEN_SECRET)
        self._producer = KafkaProducer(
            bootstrap_servers='localhost:9092', max_in_flight_requests_per_connection=5, acks='all', api_version=(0, 10), retries=100000000000)

    def stream_mentions(self):
        with open('output.txt', 'a') as f:
            for msg in self.api.GetStreamFilter(track=['@tolumide_ng'], languages=['en']):
                f.write(json.dumps(msg))
                logging.info(msg)
                self._producer.send('twitter_mentions', None, msg)
                f.write('\n')

    def stream_timeline(self, user):
        with open('timeline.txt', 'a') as f:
            statuses = self.api.GetUserTimeline(user_id=user)
            # print([s.text for s in statuses])
            for s in statuses:
                # f.write(json.dumps(s.text))
                val = bytes(s.text, encoding='utf-8')
                print(val)
                self._producer.send('twitter_timelines', val)
                f.write('\n')
        f.close()

    def get_followers(self, user):
        users = self.api.GetFriends(user)
        logging.info([u.name for u in users])


if __name__ == '__main__':
    # TwitterProducer().stream_timeline('185620309')
    TwitterProducer().stream_timeline('185620309')
    # print('we are done')
