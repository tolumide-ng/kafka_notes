#!usr/bin/env python3

import os
from kafka import KafkaProducer


class ProducerDemo:
    # producer = ''

    def __init__(self):
        self._producer = KafkaProducer(
            bootstrap_servers='localhost:9092', api_version=(0, 10))
        # key_serializer=str.encode, value_serializer=str.encode

    def exec(self, key, value):
        bkey = bytes(key, encoding='utf-8')
        for i in range(10):
            bvalue = bytes(value + ': ' + str(i), encoding='utf-8')
            self._producer.send('first_topic', key=bkey, value=bvalue)
            self._producer.send('first_topic', key=b'name', value=b'tolumide')
            self._producer.flush()
        print('we are done relaying the message')


if __name__ == '__main__':
    exec = ProducerDemo()
    exec.exec('Message', 'Hello World')
