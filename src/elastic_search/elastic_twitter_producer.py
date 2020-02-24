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
    # logging.basicConfig(level=logging.NOTSET)

    def __init__(self):
        self.api = Api(CONSUMER,
                       CONSUMER_SECRET,
                       ACCESS_TOKEN,
                       ACCESS_TOKEN_SECRET)
        self._producer = KafkaProducer(
            bootstrap_servers='localhost:9092', max_in_flight_requests_per_connection=5, acks='all',
            api_version=(0, 10), retries=100000000000, compression_type='snappy', linger_ms=20, batch_size=32*1024)

    def get_timeline(self):
        for msg in self.api.GetStreamFilter(track=['bioinformatics', 'trump']):
            print(msg)
            key = bytes(str(msg.get('id', 'id')), encoding='utf-8')
            val = bytes(str(msg.get('text', 'text')), encoding='utf-8')
            logging.info(val)
            # print(val)
            self._producer.send('twitter_home', key, val)


if __name__ == '__main__':
    TwitterProducer().get_timeline()
