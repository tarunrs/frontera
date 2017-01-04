# -*- coding: utf-8 -*-
from scrapy.linkextractors import LinkExtractor
from scrapy import signals
from scrapy.spiders import CrawlSpider, Rule


class BCSpider(CrawlSpider):
    name = 'bc'

    def strip_url(urls):
        urls = [b.replace("http://", "") for b in urls]
        urls = [b.replace("https://", "") for b in urls]
        urls = [b.replace("www.", "") for b in urls]
        urls = [b.strip("\n\/") for b in urls]
        return urls

    allowed_domains = open("seeds.txt").readlines()
    allowed_domains = strip_url(allowed_domains)
    blocked_domains = open("block.txt").readlines()
    blocked_domains = strip_url(blocked_domains)

    rules = (
        Rule(LinkExtractor(allow_domains=allowed_domains,
                           deny_domains=blocked_domains), follow=True),
    )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BCSpider, cls).from_crawler(crawler, *args, **kwargs)
        spider._set_crawler(crawler)
        spider.crawler.signals.connect(
            spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        self.log("Spider idle signal caught.")
        raise DontCloseSpider
