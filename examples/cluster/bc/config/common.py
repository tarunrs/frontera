# -*- coding: utf-8 -*-
from frontera.settings.default_settings import MIDDLEWARES

MAX_NEXT_REQUESTS = 512
SPIDER_FEED_PARTITIONS = 7
SPIDER_LOG_PARTITIONS = 1
DELAY_ON_EMPTY = 5.0

MIDDLEWARES.extend([
    'frontera.contrib.middlewares.domain.DomainMiddleware',
    'frontera.contrib.middlewares.fingerprint.DomainFingerprintMiddleware',
    'frontera.contrib.middlewares.extract.NewsDetailsExtractMiddleware',
    'frontera.contrib.middlewares.extract.EntityDetailsExtractMiddleware',
    'frontera.contrib.middlewares.index.ElasticSearchIndexMiddleware'
])

#--------------------------------------------------------
# Crawl frontier backend
#--------------------------------------------------------
QUEUE_HOSTNAME_PARTITIONING = True
URL_FINGERPRINT_FUNCTION='frontera.utils.fingerprint.hostname_local_fingerprint'
ELASTICSEARCH_SERVER="10.2.0.7"

MESSAGE_BUS='frontera.contrib.messagebus.kafkabus.MessageBus'
KAFKA_LOCATION = '10.2.0.4:9092'
SCORING_GROUP = 'scrapy-scoring'
SCORING_TOPIC = 'frontier-score'
STORE_CONTENT = True
