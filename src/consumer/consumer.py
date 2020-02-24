#!usr/bin/env python3

from kafka import KafkaConsumer
# import logging
# logging.basicConfig(level=logging.NOTSET)


class Consumer:
    consumer = KafkaConsumer('first_topic', auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), group_id='my-second-application')

    def exec(self):
        for message in Consumer.consumer:
            print(message)
            print(f'THE TOPIC =========> {message.topic}')
            print('Key: ' + str(message.key),
                  'Value: ' + str(message.value))
            print('Partition: ' + str(message.partition),
                  'Offset: ' + str(message.offset))


if __name__ == '__main__':
    Consumer().exec()
