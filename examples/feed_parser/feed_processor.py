import feedparser
from time import mktime
from datetime import datetime
import pickle
from newspaper import Article
from frontera.utils.fingerprint import hostname_local_fingerprint, sha1
from frontera.core.components import States
from frontera.contrib.backends.hbase import *
from frontera.utils.url import parse_domain_from_url_fast, parse_domain_from_url
from frontera.contrib.middlewares.extract import NewsDetailsExtractMiddleware, EntityDetailsExtractMiddleware
from frontera.contrib.middlewares.index import ElasticSearchIndexMiddleware
from frontera.contrib.middlewares.domain import DomainMiddleware
import yaml


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

  def index_in_hbase(self, item, np_item):
    obj = prepare_hbase_object(url=item["link"],
                               depth=0,
                               created_at=utcnow_timestamp(),
                               domain_fingerprint=item[b'domain_fingerprint'],
                               status_code = 200,
                               #content = np_item.html,
                               state = States.CRAWLED)
    #print obj

  def _parse(self, feed):
    doc = feedparser.parse(feed)
    for item in doc["items"]:
      res = Response(item["link"])
      res = self.de.add_domain(res)
      res.meta[b"fingerprint"] = hostname_local_fingerprint(res.url)
      item[b"domain_fingerprint"] = sha1(res.meta[b"domain"][b"name"])
      res = self.nde.add_details(res, None)    
      if res.meta[b"published_date"] is None:
        res.meta[b"published_date"] = datetime.fromtimestamp(mktime(item["published_parsed"]))
      res = self.ede.add_details(res)
      print res.meta
      self.esi.add_to_index(res)
      self.index_in_hbase(item, res)

     
  def parse(self):
    for feed in self.feeds:
      self._parse(feed)
     
if __name__ == "__main__":
   f = FeedsParser()
   f.parse()
