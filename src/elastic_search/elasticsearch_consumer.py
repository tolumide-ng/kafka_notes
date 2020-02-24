#!/usr/bin/env python3

from dotenv import load_dotenv
from datetime import datetime
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import os
import re
import uuid
import logging
import base64

load_dotenv()


class ElasticSearch:
    logging.basicConfig(level=logging.INFO)

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
        # consumer = KafkaConsumer(topic, auto_offset_reset='earliest',
        #                          bootstrap_servers=['localhost:9092'], api_version=(0, 10), group_id='kafka-demo-elasticsearch')
        consumer = KafkaConsumer(topic,
                                 bootstrap_servers=['localhost:9092'], api_version=(0, 10))
        return consumer

    def communicate(self, topic=None):
        kafka_consumer = self.create_kafka_consumer(topic)

        for message in kafka_consumer:
            # kafka_id = str(message.topic) + "_" + \
            #     str(message.partition) + "_" + str(message.offset)
            # msg_id = uuid.uuid4()

            # Insert the data into elastic search
            print('decoded', str(message.key))
            keyed = message.value.decode('utf-8')
            print('keeeeeeeeeyyyyyyyyyyy', keyed)

            print('\n')
            self.es.index(index="twitter", doc_type='tweets',
                          id=keyed, body={'time': str(message.timestamp), 'val': str(message.key), 'id': keyed})

            res = self.es.get(index="twitter", doc_type='tweets', id=keyed)
            print(res['_id'])


if __name__ == "__main__":
    ElasticSearch().communicate('twitter_home')
