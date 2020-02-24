#!/usr/bin/env python3

from dotenv import load_dotenv
from datetime import datetime
from elasticsearch import Elasticsearch
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
        # consumer = KafkaConsumer(topic, auto_offset_reset='earliest',
        #  bootstrap_servers = ['localhost:9092'], api_version = (0, 10), group_id = 'kafka-demo-elasticsearch')
        # consumer = KafkaConsumer(topic,
        #                          bootstrap_servers=['localhost:9092'], api_version=(0, 10), auto_offset_reset='earliest', max_poll_records=10, enable_auto_commit=False, group_id='kafka-demo-elasticsearch')

        # consumer = KafkaConsumer(topic,
        #                          bootstrap_servers=['localhost:9092'], api_version=(0, 10), enable_auto_commit=False, auto_offset_reset='latest', group_id='kafka-demo-elasticsearch')
        # consumer.close(autocommit=False)
        consumer = KafkaConsumer(topic, auto_offset_reset='latest',
                                 bootstrap_servers=['localhost:9092'], api_version=(0, 10), group_id='first_application', enable_auto_commit=False, max_poll_records=5)

        return consumer

    def communicate(self, topic=None):
        kafka_consumer = self.create_kafka_consumer(topic)

        while True:

            records = kafka_consumer.poll(
                timeout_ms=10000, max_records=5, update_offsets=True)

            print('\n')
            print('received ' + str(len(records)) + ' records')
            print('\n')

            if bool(records):
                for message in records.values():
                    print('\n')
                    print('type of the messagfe', type(message))
                    print('\n')

                    print(message[0])
                    print('\n')

                    keyed = message[0].value.decode('utf-8')

                    print(keyed)

                    # kafka_id = str(message.topic) + "_" + \
                    #     str(message.partition) + "_" + str(message.offset)

                    # msg_id = uuid.uuid4()

                    print('\n')
                    self.es.index(index="twitter", doc_type='tweets',
                                  id=keyed, body={'time': str(message[0].timestamp), 'val': str(message[0].key.decode('utf-8')), 'id': keyed})

                    # self.es.get(index="twitter", doc_type='tweets', id=keyed)
                    # print(res['_id'])

                # send a response of of successful/failed commit back to the producer after each loop
                print('COMMITING OFFSETS...')
                kafka_consumer.commit()
                print('<<<<<<<<<OFFSETS COMMITTED>>>>>>>>')
                print('\n')
                # print(kafka_consumer.committed(1))
                # print(kafka_consumer.committed)
                time.sleep(10)
                print('RETRYING BECAUSE LENGTH IS NOT GREATER THAN OR EQUAL TO ONE>>>>>')
                print('\n')


if __name__ == "__main__":
    ElasticSearch().communicate('twitter_home')
