#!usr/bin/env python3

from kafka import KafkaConsumer
# import logging
# logging.basicConfig(level=logging.NOTSET)


class ConsumerThreads:
    _countdown = ''

    def consumerThread(self):
        print()

    def run(self):
        print('yes l;prd')


class Consumer:
    consumer = KafkaConsumer('first_topic', auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), group_id='fourth_application')

    def exec(self):
        for message in Consumer.consumer:
            print(message)
            print(f'THE TOPIC =========> {message.topic}')
            print('Key: ' + str(message.key),
                  'Value: ' + str(message.value))
            print('Partition: ' + str(message.partition),
                  'Offset: ' + str(message.offset))

        Consumer.consumer.close()
        print('Application has exited')


if __name__ == '__main__':
    Consumer().exec()
