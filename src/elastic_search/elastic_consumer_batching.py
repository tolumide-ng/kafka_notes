
#!usr/bin/env python3
"""
This module makes efficient use of kafka's consumer polling property, although increasing latency a little but decreasing the number of requests made,
and efficiently uses ElasticSearch's batching property to load the pooled data into elastic search in batches
"""


from dotenv import load_dotenv
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk
from kafka import KafkaConsumer
import time
import os
import re
import uuid
import logging
import base64

load_dotenv()


class ElasticSearch:
    # logging.basicConfig(level=logging.INFO)

    def __init__(self):
        self.create_elastic_client()
        self.es = Elasticsearch(self.es_header)

    def create_elastic_client(self):
        self.bonsai = os.getenv('ELASTIC_ADDRESS')
        self.auth = re.search(r'https\:\/\/(.*)\@',
                              self.bonsai).group(1).split(':')
        # print(self.auth)
        self.host = self.bonsai.replace(
            'https://%s:%s@' % (self.auth[0], self.auth[1]), '')
        self.match = re.search('(:\d+)', self.host)
        if self.match:
            p = self.match.group(0)
            self.host = self.host.replace(p, '')
            self.port = int(p.split(':')[1])
        else:
            self.port = 443

        self.es_header = [{
            'host': self.host,
            'port': self.port,
            'use_ssl': True,
            'http_auth': (self.auth[0], self.auth[1])
        }]

    def create_kafka_consumer(self, topic):
        consumer = KafkaConsumer(topic, auto_offset_reset='latest',
                                 bootstrap_servers=['localhost:9092'], api_version=(0, 10), group_id='testing_tweets', enable_auto_commit=False, max_poll_records=20)

        return consumer

    def gen_data(self, records):
        for message in records.values():
            keyed = message[0].value.decode('utf-8')
            yield {
                "_id": keyed,
                "_index": "twitter",
                "doc_type": "tweets",
                "_type": "tweets",
                "doc": {"time": str(message[0].timestamp), "val": str(message[0].key.decode('utf-8')), "id": keyed},
                "_op_type": "create",
            }

    def communicate(self, topic=None):
        kafka_consumer = self.create_kafka_consumer(topic)

        while True:
            records = kafka_consumer.poll(
                timeout_ms=10000, max_records=5, update_offsets=True)

            print('\n')
            print('received ' + str(len(records)) + ' records')
            print('\n')

            if bool(records):

                # bulk load into elastic search
                bulk(self.es, self.gen_data(records), index='twitter')

                # send a response of of successful/failed commit back to the producer after each loop
                print('COMMITING OFFSETS...')
                kafka_consumer.commit()
                print('<<<<<<<<<OFFSETS COMMITTED>>>>>>>>')
                print('\n')

                time.sleep(10)
            else:
                print('RETRYING BECAUSE LENGTH IS NOT GREATER THAN OR EQUAL TO ONE>>>>>')
                print('\n')


if __name__ == "__main__":
    ElasticSearch().communicate('twitter_home')
