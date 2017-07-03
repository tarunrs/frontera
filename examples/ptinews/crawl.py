#!/usr/bin/python
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
from frontera.utils.fingerprint import hostname_local_fingerprint, sha1
from frontera.core.components import States
from frontera.contrib.backends.hbase import *
from frontera.utils.misc import get_crc32
from frontera.contrib.middlewares.extract import EntityDetailsExtractMiddleware
from frontera.contrib.middlewares.index import ElasticSearchIndexMiddleware
from frontera.contrib.middlewares.domain import DomainMiddleware
from binascii import unhexlify
import yaml
import happybase
import logging


logfile = "/home/cia/bitbucket/frontera/examples/ptinews/crawl.log"
configfile = "/home/cia/bitbucket/frontera/examples/ptinews/config.yaml"

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


class PTICrawler:

    def __init__(self):
        logging.basicConfig(filename=logfile, level=logging.INFO, format='%(asctime)s %(message)s')
        self.manager = Manager()
        hb_host = self.manager.settings.get("HBASE_THRIFT_HOST")
        hb_port = int(self.manager.settings.get("HBASE_THRIFT_PORT"))
        hb_timeout = int(self.manager.settings.get("HBASE_TIMEOUT"))
        self.hb_connection = happybase.Connection(
            host=hb_host, port=hb_port, timeout=hb_timeout)
        self.hb_table = self.hb_connection.table("crawler:metadata")

        self.feeds = []
        self.ede = EntityDetailsExtractMiddleware(None)
        self.esi = ElasticSearchIndexMiddleware(self.manager)
        self.de = DomainMiddleware(self.manager)

    def index_in_hbase(self, response):
        domain_fingerprint = sha1(response.meta[b"domain"][b"name"])
        obj = prepare_hbase_object(url=response.url,
                                   depth=0,
                                   created_at=utcnow_timestamp(),
                                   domain_fingerprint=domain_fingerprint,
                                   status_code=200,
                                   content=response.meta[b'html'],
                                   state=States.CRAWLED)
        self.hb_table.put(unhexlify(response.meta[b'fingerprint']), obj)

    def get_article_details(self, url):
        res = requests.get(url)
        html = res.text
        doc = BeautifulSoup(html)
        title = doc.find("div", {"id": "dvheadin"}).get_text()
        published_date = doc.findAll("font", {"class": "fullstorytime"})[
            1].get_text()
        published_date = datetime.strptime(
            published_date, "%m/%d/%Y %I:%M:%S %p")
        body = doc.find("div", {"class": "fulstorytext"}).get_text()
        l = body.split("\r\n")
        if "By" in l[0]:
            text = " ".join(l[1:])
            author = l[0].replace("By ", "")
        else:
            text = " ".join(l)
            author = None
        return title, text, html, author, published_date

    def get_paginated_links(self, session, doc):
        page_link = doc.find("a", {"name": "hpaging"})
        temp = page_link["onclick"][15:-2]
        temp = temp.split(",")
        start_page = int(temp[0][1:-1])
        end_page = int(temp[1][1:-1])
        count = int(temp[2][1:-1])
        links = []
        for i in range(start_page + 1, end_page + 1):
            params = dict()
            params["chkids"] = "0"
            params["count"] = str(end_page)
            params["pagewidth"] = str(count)
            params["pnum"] = str(i)
            url = "http://www.ptinews.com/news/newnewslisting.aspx/changeData"
            res = session.post(url, data=json.dumps(params), headers={
                               "Referer": "http://www.ptinews.com/news/newnewslisting.aspx?btnid=subscription", "Host": "www.ptinews.com", 'content-type': 'application/json'})
            html = json.loads(res.text).split("^")[1]
            sub_doc = BeautifulSoup(html)
            news_tds = sub_doc.findAll("td", {"class": "news_head_download"})
            for td in news_tds:
                temp = td["onclick"][20:-2]
                temp = temp.split(",")
                url = "http://www.ptinews.com/news/"
                url += temp[0][1:-1] + "__" + \
                    temp[1][1:-1] + "$" + temp[2][1:-1]
                links.append(url)
        return links

    def get_news_links(self):
        url = "http://www.ptinews.com/"
        s = requests.session()
        r = s.get(url)
        doc = BeautifulSoup(r.text)
        req_elements = set(["__EVENTTARGET", "__EVENTARGUMENT", "__VIEWSTATE", "__VIEWSTATEGENERATOR",
                            "ctl00$txtusrname", "ctl00$txtpwd", "ctl00$txtuval", "ctl00$btnlogin", "ctl00$txtlogintop"])
        input_elements = doc.findAll("input")
        params = dict()
        for el in input_elements:
            if el.get("name") in req_elements:
                params[el["name"]] = el.get("value")
        params["ctl00$txtusrname"] = "news@crediwatch.com"
        params["ctl00$txtpwd"] = "news#69"
        url2 = "http://www.ptinews.com/home.aspx"
        r = s.post(url2, params)
        url2 = "http://www.ptinews.com/main.aspx"
        r = s.get(url2)
        url3 = "http://www.ptinews.com/news/newnewslisting.aspx?btnid=subscription"
        r = s.get(url3)
        doc = BeautifulSoup(r.text)
        news_tds = doc.findAll("td", {"class": "news_head_download"})
        links = []
        for td in news_tds:
            temp = td["onclick"][20:-2]
            temp = temp.split(",")
            url = "http://www.ptinews.com/news/"
            url += temp[0][1:-1] + "__" + temp[1][1:-1] + "$" + temp[2][1:-1]
            links.append(url)
        return s, doc, links

    def logout(self, session, doc):
        url = "http://www.ptinews.com/news/newnewslisting.aspx?btnid=subscription"
        input_elements = doc.findAll("input")
        req_elements = set(["__EVENTTARGET", "__EVENTARGUMENT", "__VIEWSTATEGENERATOR", "ctl00$txtuval", "ctl00$btnlogout.x", "ctl00$btnlogout.y",
                            "ctl00$ContentPlaceHolder1$engtxt", "ctl00$ContentPlaceHolder1$count_cat", "ctl00$ContentPlaceHolder1$count_date", "ctl00$ContentPlaceHolder1$count_src"])
        params = dict()
        for el in input_elements:
            if el.get("name") in req_elements:
                params[el["name"]] = el.get("value")
        r = session.post(url, params)

    def process(self):
        logging.info("Logging in")
        session, doc, links = self.get_news_links()
        try:
            logging.info("Logged in. Getting paginated links")
            res = session.get("http://www.ptinews.com/updatesession.aspx")
            links += self.get_paginated_links(session, doc)
        except Exception as e:
            logging.error(e)

        logging.info("Crawling and parsing articles. Found: %s", str(len(links)))
        for url in links:
            logging.info("Crawling: %s", url)
            try:
                res = Response(url)
                res = self.de.add_domain(res)
                res.meta[b"fingerprint"] = hostname_local_fingerprint(res.url)
                title, text, html, author, published_date = self.get_article_details(
                    url)
                res.meta[b"text"] = text
                res.meta[b"content_hash"] = get_crc32(text)
                res.meta[b"title"] = title
                res.meta[b"html"] = html
                res.meta[b"published_date"] = published_date
                res.meta[b"crawled_date"] = datetime.now()
                res.meta[b"image"] = None
                res.meta[b"authors"] = author
                res = self.ede.add_details(res)
                self.index_in_hbase(res)
                self.esi.add_to_index(res)
            except Exception as e:
               logging.error(e)
        self.logout(session, doc)

if __name__ == "__main__":
    p = PTICrawler()
    p.process()
