#!usr/bin/env python3

from kafka import KafkaConsumer, TopicPartition
# import logging
# logging.basicConfig(level=logging.NOTSET)


class ConsumerThreads:
    _countdown = ''

    def consumerThread(self):
        print()

    def run(self):
        print('yes l;prd')


class Consumer:

    def __init__(self):
        self.consumer = KafkaConsumer(auto_offset_reset='earliest',
                                      bootstrap_servers=['localhost:9092'], api_version=(0, 10),)
        self.topic = 'first_topic'

    def exec(self):
        self.topicPartition = TopicPartition(self.topic, partition=0)

        # self.consumer.assign(
        # [TopicPartition(topic=self.topic, partition=partition) for partition in other_partition])

        # self.consumer.seek(TopicPartition(topic=self.topic, partition=partition), partition_offset + 1)

        self.consumer.assign(self.topicPartition)

        self.consumer.seeek(TopicPartition(
            topic=self.topic, partition=0), partition_offset=12)

        # assign

        for message in self.consumer:
            print(message)
            print(f'THE TOPIC =========> {message.topic}')
            print('Key: ' + str(message.key),
                  'Value: ' + str(message.value))
            print('Partition: ' + str(message.partition),
                  'Offset: ' + str(message.offset))

        self.consumer.close()
        print('Application has exited')


if __name__ == '__main__':
    Consumer().exec()
