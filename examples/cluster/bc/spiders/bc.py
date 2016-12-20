# -*- coding: utf-8 -*-
from scrapy.spider import Spider
from scrapy.http import Request
from scrapy.http.response.html import HtmlResponse
from scrapy.linkextractors import LinkExtractor
from scrapy import signals
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class BCSpider(CrawlSpider):
    name = 'bc'
    a = open("seeds.txt").readlines()
    a = [b.replace("http://", "") for b in a]
    a = [b.replace("www.", "") for b in a]
    a = [b.strip("\n\/") for b in a]
    allowed_domains = a

    rules = (
        Rule(LinkExtractor(allow_domains=allowed_domains), follow=True),
    )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BCSpider, cls).from_crawler(crawler, *args, **kwargs)
        spider._set_crawler(crawler)
        spider.crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        self.log("Spider idle signal caught.")
        raise DontCloseSpider
