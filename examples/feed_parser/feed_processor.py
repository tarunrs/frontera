import feedparser
from time import mktime, sleep
from datetime import datetime
from frontera.utils.fingerprint import hostname_local_fingerprint, sha1
from frontera.core.components import States
from frontera.contrib.backends.hbase import *
from frontera.contrib.middlewares.extract import NewsDetailsExtractMiddleware, EntityDetailsExtractMiddleware
from frontera.contrib.middlewares.index import ElasticSearchIndexMiddleware
from frontera.contrib.middlewares.domain import DomainMiddleware
from binascii import unhexlify
from elasticsearch_dsl.connections import connections
import yaml
import happybase
import sys
import logging
import pickle
import os
from random import shuffle


class Manager:

    def __init__(self):
        self.settings = dict()
        os.chdir(os.path.dirname(__file__))
        self.settings = yaml.safe_load(open("config.yaml"))
        self.test_mode = False


class Response:

    def __init__(self, url):
        self.url = url
        self.body = ""
        self.meta = dict()


class FeedsParser:

    def __init__(self, logfile):
        self.manager = Manager()
        logging.basicConfig(filename=logfile, level=logging.INFO,
                            format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(lineno)d %(message)s')
        logging.getLogger("elasticsearch_dsl").setLevel(logging.WARNING)
        logging.getLogger("newspaper").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("elasticsearch").setLevel(logging.WARNING)
        self.logger = logging.getLogger(__name__)
        self.hb_connection = None
        self.hb_table = None
        self.establish_hbase_connection()
        self.es_client = connections.create_connection(
            hosts=self.manager.settings.get('ELASTICSEARCH_SERVER', ["localhost"]), timeout=30)

        self.feeds = []
        self.nde = NewsDetailsExtractMiddleware(self.manager)
        self.ede = EntityDetailsExtractMiddleware(None)
        self.esi = ElasticSearchIndexMiddleware(self.manager)
        self.de = DomainMiddleware(self.manager)
        self.new_links_count = 0
        self.total_links_count = 0
        self.url_hash_cache = dict()
        self.use_url_cache = False

        with open(self.manager.settings.get("FEED_FILE")) as f:
            self.feeds = f.readlines()
            self.feeds = [el.strip("\n") for el in self.feeds]

    def establish_hbase_connection(self):
        hb_host = self.manager.settings.get("HBASE_THRIFT_HOST")
        hb_port = int(self.manager.settings.get("HBASE_THRIFT_PORT"))
        hb_timeout = int(self.manager.settings.get("HBASE_TIMEOUT"))
        self.hb_connection = happybase.Connection(
            host=hb_host, port=hb_port, timeout=hb_timeout)
        self.hb_table = self.hb_connection.table("crawler:metadata")

    def load_url_cache(self, partition_num):
        try:
            self.logger.info(
                "Loading URL cache for partition: " + str(partition_num))
            filename = self.manager.settings.get(
                "CACHE_LOCATION") + "url_cache_" + str(partition_num) + ".pkl"
            self.prev_url_hash_cache = pickle.load(open(filename))
            self.use_url_cache = True
            self.logger.info("Using URL cache")
        except Exception as e:
            self.use_url_cache = False
            self.logger.exception(str(e))
            self.logger.info(
                "URL cache could not be loaded. Using elasticsearch index")

    def index_in_hbase(self, response):
        domain_fingerprint = sha1(response.meta[b"domain"][b"name"])
        obj = prepare_hbase_object(url=response.url,
                                   depth=0,
                                   created_at=utcnow_timestamp(),
                                   domain_fingerprint=domain_fingerprint,
                                   status_code=200,
                                   content=response.meta[b'html'],
                                   state=States.CRAWLED)
        try:
            self.hb_table.put(unhexlify(response.meta[b'fingerprint']), obj)
        except Exception as e:
            self.logger.exception(str(e))
            self.logger.info("Retrying in 30 seconds")
            sleep(30)
            self.establish_hbase_connection()
            self.index_in_hbase(response)

    def already_indexed(self, response):
        try:
            if self.use_url_cache:
                if self.prev_url_hash_cache.get(response.meta[b'fingerprint']) is not None:
                    return True
                else:
                    return False
            doc = self.es_client.get(
                id=response.meta[b"fingerprint"], index="news")
            return True
        except:
            return False

    def update_cache(self, response):
        self.url_hash_cache[response.meta[b"fingerprint"]] = True

    def _parse(self, feed):
        doc = feedparser.parse(feed)
        self.total_links_count += len(doc["items"])
        self.logger.info("Parsing: %s, Found: %s",
                         feed, str(len(doc["items"])))
        links_count = len(doc["items"])
        new_links = 0
        for item in doc["items"]:
            try:
                res = Response(item["link"])
                res = self.de.add_domain(res)
                res.meta[b"fingerprint"] = hostname_local_fingerprint(res.url)
                if self.already_indexed(res):
                    self.update_cache(res)
                    continue
                res = self.nde.add_details(res, None)
                try:
                    if res.meta[b"published_date"] is None and item.get("published_parsed") is not None:
                        res.meta[b"published_date"] = datetime.fromtimestamp(
                            mktime(item["published_parsed"]))
                except Exception as e:
                    self.logger.exception(e)
                res = self.ede.add_details(res)
                try:
                    self.index_in_hbase(res)
                except Exception as e:
                    self.logger.exception(str(e))
                self.esi.add_to_index(res)
                self.update_cache(res)
                new_links += 1
                self.new_links_count += 1
            except Exception as e:
                if item.get("link"):
                    self.logger.error("Error parsing:" + item["link"])
                    self.logger.exception(e)
                else:
                    self.logger.exception(e)
        self.logger.info("Found %s links, %s new",
                         str(links_count), str(new_links))
        return

    def parse(self, partition_num, total_partitions):
        self.logger.info("Parsing partition %s of %s", str(
            partition_num), str(total_partitions))
        self.load_url_cache(partition_num)
        num_feeds = len(self.feeds)
        partition_size = num_feeds / total_partitions
        start_index = partition_num * partition_size
        if partition_num + 1 == total_partitions:
            end_index = num_feeds
        else:
            end_index = start_index + partition_size
        urls_to_process = self.feeds[start_index:end_index]
        shuffle(urls_to_process)
        for idx, feed in enumerate(urls_to_process):
            try:
                self._parse(feed)
                self.logger.info("Processed %d of %d", idx + 1,
                                 end_index - start_index)
            except Exception as e:
                self.logger.exception(str(e) + ": " + feed)
        self.logger.info("Found %s total links, %s new", str(
            self.total_links_count), str(self.new_links_count))
        filename = self.manager.settings.get(
            "CACHE_LOCATION") + "url_cache_" + str(partition_num) + ".pkl"
        self.logger.info("Dumping URL cache: " + filename)
        pickle.dump(self.url_hash_cache, open(filename, "wb"))
        self.logger.info("Done")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print " [ERROR] Usage: python feed_processor.py <total_partitions> <partition_number> <logfile>"
        sys.exit()
    if int(sys.argv[2]) >= int(sys.argv[1]):
        print " [ERROR] partition_number cannot be more than total_partitions"
        sys.exit()
    total_partitions = int(sys.argv[1])
    partition_num = int(sys.argv[2])
    f = FeedsParser(sys.argv[3])
    f.parse(partition_num, total_partitions)
