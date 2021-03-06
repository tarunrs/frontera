#!/usr/bin/python
__version__ = '0.1'

import urllib2

from gzip import GzipFile
from cStringIO import StringIO

try:
    import xml.etree.cElementTree as xml_parser
except ImportError:
    import xml.etree.ElementTree as xml_parser
import dateutil.parser
import time
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
from random import shuffle

logfolder = "/home/cia/bitbucket/frontera/examples/sitemap_processor/"
configfile = "/home/cia/bitbucket/frontera/examples/sitemap_processor/config.yaml"

DEFAULT_USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) sitemap-parser/{}'.format(
    __version__)

NAMESPACES = (
    '{http://www.google.com/schemas/sitemap/0.84}',
    '{http://www.google.com/schemas/sitemap/0.9}',
    '{http://www.sitemaps.org/schemas/sitemap/0.9}',
    '{http://www.google.com/schemas/sitemap-news/0.9}',
    '{http://www.google.com/schemas/sitemap-image/1.1}'
)


class SitemapParser(object):

    def __init__(self):
        self._sitemap_urls = list()

    @staticmethod
    def _plain_tag(tag):
        ''' remove namespaces and returns tag '''

        for namespace in NAMESPACES:
            if tag.find(namespace) >= 0:
                return tag.replace(namespace, '')
        return tag

    def _get(self, url):
        ''' get sitemap, if it compressed -> decompress'''
        SUPPORTED_PLAIN_CONTENT_TYPE = (
            'text/xml', 'application/xml',
        )
        SUPPORTED_COMPESSED_CONTENT_TYPE = (
            'application/octet-stream', 'application/x-gzip',
        )

        opener = urllib2.build_opener()
        urllib2.install_opener(opener)

        req = urllib2.Request(url, headers={'User-Agent': DEFAULT_USER_AGENT})
        try:
            resp = urllib2.urlopen(req, timeout=30)
        except urllib2.URLError, err:
            raise RuntimeError(err)

        if resp.code == 200:
            if resp.headers.get('content-type') is None:
                return GzipFile(fileobj=StringIO(resp.read())).read()
            content_type = resp.headers['content-type'].lower()
            if ';' in content_type:
                content_type = content_type.split(';')[0]
            if content_type in SUPPORTED_PLAIN_CONTENT_TYPE:
                return resp.read()
            elif content_type in SUPPORTED_COMPESSED_CONTENT_TYPE:
                return GzipFile(fileobj=StringIO(resp.read())).read()
        return None

    def parse_string(self, sitemap):
        ''' parse sitemap from string

        parse sitemap if there's urlset, returns the list of url details:
        loc (required), lastmod (optional), changefreq (optional), priority (optional) '''

        root = xml_parser.fromstring(sitemap)

        if self._plain_tag(root.tag) == 'sitemapindex':
            for sitemap in root:
                url = dict([(self._plain_tag(param.tag), param.text)
                            for param in sitemap])
                self._sitemap_urls.append(url['loc'])

        if self._plain_tag(root.tag) == 'urlset':
            for url in root:
                data = dict()
                for param in url:
                    tag = self._plain_tag(param.tag)
                    if tag == "news" or tag == "image":
                        data[tag] = dict()
                        for sub_param in param:
                            if sub_param.text:
                                sub_tag = self._plain_tag(sub_param.tag)
                                data[tag][
                                    sub_tag] = sub_param.text.strip(" \n")
                                if sub_tag == "publication_date" and data[tag][sub_tag] and len(data[tag][sub_tag]) > 0:
                                    try:
                                        data[tag][sub_tag] = dateutil.parser.parse(
                                            data[tag][sub_tag])
                                    except:
                                        data[tag][sub_tag] = None
                    else:
                        data[tag] = param.text
                yield data

    def parse_url(self, sitemap_url):
        ''' parse sitemap from url'''
        self._sitemap_urls.append(sitemap_url)

        while True:
            try:
                sm_url = self._sitemap_urls.pop()
            except IndexError:
                break

            sm_content = self._get(sm_url)
            if sm_content:
                for url in self.parse_string(sm_content):
                    yield url


class Manager:

    def __init__(self):
        self.settings = dict()
        self.settings = yaml.safe_load(open(configfile))
        self.test_mode = False


class Response:

    def __init__(self, url):
        self.url = url
        self.body = ""
        self.meta = dict()


class SitemapsParser(object):

    def __init__(self, logfile):
        self._sitemap_urls = list()
        self.manager = Manager()
        logging.basicConfig(filename=logfolder + logfile, level=logging.INFO,
                            format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(lineno)d %(message)s')
        logging.getLogger("elasticsearch_dsl").setLevel(logging.ERROR)
        logging.getLogger("newspaper").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("elasticsearch").setLevel(logging.ERROR)
        self.logger = logging.getLogger(__name__)
        self.hb_connection = None
        self.hb_table = None
        self.establish_hbase_connection()
        self.es_client = connections.create_connection(
            hosts=self.manager.settings.get('ELASTICSEARCH_SERVER', ["localhost"]), timeout=30)

        self.nde = NewsDetailsExtractMiddleware(self.manager)
        self.ede = EntityDetailsExtractMiddleware(None)
        self.esi = ElasticSearchIndexMiddleware(self.manager)
        self.de = DomainMiddleware(self.manager)
        self.parser = SitemapParser()
        self.new_links_count = 0
        self.global_new_links_count = 0
        self.total_links_count = 0
        self.global_total_links_count = 0
        self.url_hash_cache = dict()
        self.use_url_cache = False

        with open(logfolder + self.manager.settings.get("SITEMAPS_FILE")) as f:
            self._sitemap_urls = f.readlines()
            self._sitemap_urls = [el.strip("\n") for el in self._sitemap_urls]

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
            time.sleep(30)
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

    def _parse(self, url):
        self.logger.info("Parsing: %s", url)
        self.new_links_count = 0
        self.total_links_count = 0
        for item in self.parser.parse_url(url):
            self.total_links_count += 1
            try:
                res = Response(item["loc"])
                res = self.de.add_domain(res)
                res.meta[b"fingerprint"] = hostname_local_fingerprint(res.url)
                if self.already_indexed(res):
                    self.update_cache(res)
                    continue
                res = self.nde.add_details(res, None)
                try:
                    if item.get("news") and item["news"].get("publication_date"):
                        res.meta[b"published_date"] = item[
                            "news"]["publication_date"]
                    if item.get("news") and item["news"].get("title"):
                        res.meta[b"title"] = item["news"]["title"]
                    if item.get("image") and item["image"].get("loc"):
                        res.meta[b"image"] = item["image"]["loc"]
                except Exception as e:
                    self.logger.exception(str(e) + " : " + res.url)
                res = self.ede.add_details(res)
                try:
                    self.index_in_hbase(res)
                except Exception as e:
                    self.logger.exception(
                        "Error while indexing in HBase: %s, %s", res.url, str(e))
                self.esi.add_to_index(res)
                self.update_cache(res)
                self.new_links_count += 1
            except Exception as e:
                self.logger.exception(str(e) + " : " + url + " " + item["loc"])
        return

    def parse(self, partition_num, total_partitions):
        self.logger.info("Parsing partition %s of %s", str(
            partition_num), str(total_partitions))
        self.load_url_cache(partition_num)
        num_feeds = len(self._sitemap_urls)
        partition_size = num_feeds / total_partitions
        start_index = partition_num * partition_size
        if partition_num + 1 == total_partitions:
            end_index = num_feeds
        else:
            end_index = start_index + partition_size
        urls_to_process = self._sitemap_urls[start_index:end_index]
        shuffle(urls_to_process)

        for idx, url in enumerate(urls_to_process):
            try:
                self._parse(url)
            except Exception as e:
                self.logger.exception(
                    "Error while parsing: %s %s", url, str(e))
            self.logger.info("(%d/%d) Found %d links, %d new", idx + 1, end_index - start_index,
                             self.total_links_count, self.new_links_count)
            self.global_total_links_count += self.total_links_count
            self.global_new_links_count += self.new_links_count

        self.logger.info("Found %d total links, %d new",
                         self.global_total_links_count, self.global_new_links_count)
        filename = self.manager.settings.get(
            "CACHE_LOCATION") + "url_cache_" + str(partition_num) + ".pkl"
        self.logger.info("Dumping URL cache: " + filename)
        pickle.dump(self.url_hash_cache, open(filename, "wb"))
        self.logger.info("Done")


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print " [ERROR] Usage: python sitemap_parser.py <total_partitions> <partition_number> <logfile>"
        sys.exit()
    if int(sys.argv[2]) >= int(sys.argv[1]):
        print " [ERROR] partition_number cannot be more than total_partitions"
        sys.exit()
    total_partitions = int(sys.argv[1])
    partition_num = int(sys.argv[2])
    s = SitemapsParser(sys.argv[3])
    s.parse(partition_num, total_partitions)
