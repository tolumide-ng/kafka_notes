#!usr/bin/env python3

import os
import logging
from kafka import KafkaProducer


class ProducerDemo:
    logging.basicConfig(filename='file.logger', level=logging.NOTSET)

    def __init__(self):
        self._producer = KafkaProducer(
            bootstrap_servers='localhost:9092', api_version=(0, 10))
        # key_serializer=str.encode, value_serializer=str.encode

    def on_send_success(self, record_metadata):
        logging.debug(record_metadata.topic)
        logging.debug(record_metadata.partition)
        logging.debug(record_metadata.offset)

    def on_send_error(self, excp):
        logging.error('I am an errback', exc_info=excp)

    def exec(self, key, value):
        bkey = bytes(key, encoding='utf-8')
        for i in range(10):
            bvalue = bytes(value + ': ' + str(i), encoding='utf-8')
            self._producer.send('first_topic', key=bkey, value=bvalue)
            self._producer.send('first_topic', key=b'name',
                                value=b'tolumide').add_callback(self.on_send_success).add_errback(self.on_send_error)
            self._producer.flush()


if __name__ == '__main__':
    exec = ProducerDemo()
    exec.exec('Message', 'Hello World')
