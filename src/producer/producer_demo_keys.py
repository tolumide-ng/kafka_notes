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

    def exec(self, svalue):
        topic = 'first_topic'

        for i in range(10):
            key = bytes('id_' + ': ' + str(i), encoding='utf-8')
            value = bytes(svalue + ': ' + str(i), encoding='utf-8')
            self._producer.send(topic, key, value).add_callback(
                self.on_send_success).add_errback(self.on_send_error)

            # self._producer.send(topic, key=b'name',
            #                     value=b'tolumide')
            self._producer.flush()


if __name__ == '__main__':
    exec = ProducerDemo()
    exec.exec('Hello World')
