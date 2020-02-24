#!/usr/bin/env python3

from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from datetime import datetime
import os
import re
import logging
import base64
load_dotenv()


class ElasticSearch:
    logging.basicConfig(level=logging.INFO)

    doc = {
        'author': 'kimchy',
        'text': 'Elasticsearch: cool. bonsai cool.',
        'timestamp': datetime.now(),
    }

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

    def communicate(self, info=None):
        if info is None:
            info = ElasticSearch.doc
        self.es.index(index="twitter",
                            doc_type='tweets', id=1, body=info)

        rep = self.es.get(index="twitter", doc_type='tweets', id=1)
        print(rep)
        print(rep['_source'])


if __name__ == "__main__":
    ElasticSearch().communicate()
