## APACHE KAFKA 
- This is a basic introduction to apache kafka course, written in python.
- The project makes use of dependencies such as:
    1. [Kafka-Python](https://pypi.org/project/kafka-python/)
    2. [Kafka-twitter](https://python-twitter.readthedocs.io/en/latest/index.html)
    3. [Elastic-Searcg](https://www.elastic.co/)


- Employed use cases:
1. Source ==> Kafka (Producer API)
2. Kafka ==> Kafka (Producer => Consumer)
3. Kafka ==> Sink
4. Kafka ==> App (Consumer => Elastic Search)



## Directions
1. Start Zookeeper - `bin/zookeeper-server-start.sh config/zookeeper.properties`
2. Start Kafka - `kafka-server-start.sh config/server.properties`
3. Run any of the modules in the directory



### Usecase:
Twitter => Kafka(Producer) => Kafka(Consumer) => ElasticSearch
Needs:
 - Ensure you provide all neede secrets as provided in the `.env.sample` file
1. `cd src`
2. `cd elastic_search`
3. Open terminal/zsh/fish ... and 
    i. `python3`
    ii. `from elastic_twitter_producer import TwitterProducer as tp`
    iii. `tp().get_timeline()`
4. Open another terminal and run any of the other consumers in the elastic_search dir



# kafka_notes
