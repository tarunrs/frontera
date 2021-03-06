# -*- coding: utf-8 -*-
FRONTERA_SETTINGS = 'bc.config.spider'

SCHEDULER = 'frontera.contrib.scrapy.schedulers.frontier.FronteraScheduler'
SPIDER_MIDDLEWARES = {
    'frontera.contrib.scrapy.middlewares.schedulers.SchedulerSpiderMiddleware': 999,
    'frontera.contrib.scrapy.middlewares.seeds.file.FileSeedLoader': 1,
}
DOWNLOADER_MIDDLEWARES = {
    'frontera.contrib.scrapy.middlewares.schedulers.SchedulerDownloaderMiddleware': 999,
    'frontera.contrib.scrapy.middlewares.robots.RobotsCrawlDelayMiddleware': 100,
}
EXTENSIONS = {
    'scrapy_jsonrpc.webservice.WebService': 500,
}

BOT_NAME = 'crediwatchbot (www.crediwatch.com)'

SPIDER_MODULES = ['bc.spiders']
NEWSPIDER_MODULE = 'bc.spiders'

CONCURRENT_REQUESTS=256
CONCURRENT_REQUESTS_PER_DOMAIN=1


ROBOTS_CRAWLDELAY_ENABLED = True
ROBOTS_CRAWLDELAY_VERBOSE = True  # enable stats

DOWNLOAD_DELAY=0.0
DOWNLOAD_TIMEOUT=180
RANDOMIZE_DOWNLOAD_DELAY = False

REACTOR_THREADPOOL_MAXSIZE = 30
DNS_TIMEOUT = 120

COOKIES_ENABLED=False
RETRY_ENABLED = False
REDIRECT_ENABLED = True
AJAXCRAWL_ENABLED = True

AUTOTHROTTLE_ENABLED=False
AUTOTHROTTLE_START_DELAY=0.01
AUTOTHROTTLE_MAX_DELAY = 3.0
AUTOTHROTTLE_DEBUG=False

LOG_LEVEL='INFO'

ROBOTSTXT_OBEY = True
ROBOTSTXT_ENABLED = True

JSONRPC_ENABLED = True
JSONRPC_HOST='0.0.0.0'
JSONRPC_PORT = [6080, 6081]
