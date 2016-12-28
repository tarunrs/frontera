import feedparser
from time import mktime
from datetime import datetime
import pickle
from frontera.utils.fingerprint import hostname_local_fingerprint, sha1
from frontera.core.components import States
from frontera.contrib.backends.hbase import *
from frontera.contrib.middlewares.extract import NewsDetailsExtractMiddleware, EntityDetailsExtractMiddleware
from frontera.contrib.middlewares.index import ElasticSearchIndexMiddleware
from frontera.contrib.middlewares.domain import DomainMiddleware
from binascii import unhexlify
import yaml
import happybase

class Manager:

    def __init__(self):
        self.settings = dict()
        self.settings = yaml.safe_load(open("config.yaml"))
        self.test_mode = False


class Response:

    def __init__(self, url):
        self.url = url
        self.body = ""
        self.meta = dict()


class FeedsParser:

    def __init__(self):
        self.manager = Manager()
        hb_host = self.manager.settings.get("HBASE_THRIFT_HOST")
        hb_port = int(self.manager.settings.get("HBASE_THRIFT_PORT"))
        hb_timeout = int(self.manager.settings.get("HBASE_TIMEOUT"))
        print hb_host, hb_port, hb_timeout
        self.hb_connection = happybase.Connection(host=hb_host, port=hb_port, timeout=hb_timeout)
        self.hb_table = self.hb_connection.table("crawler:metadata")

        self.feeds = []
        self.nde = NewsDetailsExtractMiddleware(None)
        self.ede = EntityDetailsExtractMiddleware(None)
        self.esi = ElasticSearchIndexMiddleware(self.manager)
        self.de = DomainMiddleware(self.manager)

        with open(self.manager.settings.get("FEED_FILE")) as f:
            self.feeds = f.readlines()
            self.feeds = [el.strip("\n") for el in self.feeds]
        try:
            self.feed_dates = pickle.load(open("feed-dates.pkl"))
        except:
            self.feed_dates = {}

    def index_in_hbase(self, response):
        domain_fingerprint = sha1(response.meta[b"domain"][b"name"])
        obj = prepare_hbase_object(url=response.url,
                                   depth=0,
                                   created_at=utcnow_timestamp(),
                                   domain_fingerprint=domain_fingerprint,
                                   status_code=200,
                                   content = response.meta[b'html'],
                                   state=States.DEFAULT)
        self.hb_table.put(unhexlify(response.meta[b'fingerprint']), obj)
        print obj

    def _parse(self, feed):
        doc = feedparser.parse(feed)
        for item in doc["items"]:
            res = Response(item["link"])
            res = self.de.add_domain(res)
            res.meta[b"fingerprint"] = hostname_local_fingerprint(res.url)
            res = self.nde.add_details(res, None)
            if res.meta[b"published_date"] is None:
                res.meta[b"published_date"] = datetime.fromtimestamp(
                    mktime(item["published_parsed"]))
            res = self.ede.add_details(res)
            self.index_in_hbase(res)
            self.esi.add_to_index(res)

    def parse(self):
        for feed in self.feeds:
            self._parse(feed)

if __name__ == "__main__":
    f = FeedsParser()
    f.parse()
