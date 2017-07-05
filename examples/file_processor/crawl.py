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


class FileProcessor:

    def __init__(self):
        self.manager = Manager()
        self.urls = []
        self.nde = NewsDetailsExtractMiddleware(None)
        self.ede = EntityDetailsExtractMiddleware(None)
        self.esi = ElasticSearchIndexMiddleware(self.manager)
        self.de = DomainMiddleware(self.manager)

        with open("urls.csv") as f:
            self.urls = f.readlines()
            self.urls = [el.strip("\n") for el in self.urls]

    def parse(self):
        domains = []
        for url in self.urls:
            try:
                res = Response(url)
                res = self.de.add_domain(res)
                res.meta[b"fingerprint"] = hostname_local_fingerprint(res.url)
                res = self.nde.add_details(res, None)
                res = self.ede.add_details(res)
                domains.append(res.meta[b"domain"][b'netloc'])
                print res.meta[b"domain"][b'netloc']
                self.esi.add_to_index(res)
            except Exception as e:
                print " [ERROR]", e  
        domains = list(set(domains))
        domains.sort()
        s = "\n".join(domains)
        f = open("domains.csv", "w")
        f.write(s)
        f.close()


if __name__ == "__main__":
    f = FileProcessor()
    f.parse()
    print " [INFO] Done"
