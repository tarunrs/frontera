from __future__ import absolute_import
from frontera.core.components import Middleware
from boilerpipe.extract import Extractor
import datefinder
import articleDateExtractor
from bs4 import BeautifulSoup
import datetime


class BaseExtractMiddleware(Middleware):
    component_name = 'Base Extract Middleware'

    def __init__(self, manager):
        pass

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        return seeds

    def page_crawled(self, response):
        return self._add_news_details(response)

    def links_extracted(self, request, links):
        return request

    def request_error(self, request, error):
        return request

    def _add_news_details(self, obj):
        raise NotImplementedError


class NewsDetailsExtractMiddleware(BaseExtractMiddleware):
    """
    This :class:`Middleware <frontera.core.components.Middleware>` will add a 'text', 'description', 
    'published_date', 'crawled_date' field for every
    :attr:`Response.meta <frontera.core.models.Response.meta>` if is activated.
    """

    component_name = 'News Details Extract Middleware'

    def get_title(self, doc):
        title = None
        try:
            title = doc.find("title").get_text()
        except:
            title = None
        return title


    def find_in_list_of_attribute_type(self, doc, tag, attributes):
        date = None
        for attribute in attributes:
            for a_type in attributes[attribute]:
                temp = doc.findAll(tag, {attribute: a_type})
                if len(temp) > 0:
                    try:
                        date = temp[0]["content"]
                        if len(date) > 0:
                            return date
                    except:
                        continue
        return date

    def get_published_date(self, doc):
        tag = "meta"
        match = None
        attributes = {
            "property": ["article:published_time", "article:modified_time",
                         "og:article:published_time", "og:article:modified_time"],
            "itemprop": ["datePublished", "dateModified"],
            "http-equiv": ["Last-Modified"]
        }
        try:
            temp = self.find_in_list_of_attribute_type(doc, tag, attributes)
            matches = datefinder.find_dates(temp)
            for match in matches:
                return match
        except:
            match = None
        return match

    def _add_news_details(self, obj):
        extractor = Extractor(extractor='ArticleExtractor', html=obj.body)
        obj.meta[b"text"] = extractor.getText()
        doc = BeautifulSoup(obj.body)
        obj.meta[b"title"] = self.get_title(doc)
        obj.meta[b"published_date"] = self.get_published_date(doc)
        if obj.meta[b"published_date"] is None:
            obj.meta[b"published_date"] = articleDateExtractor.extractArticlePublishedDate(
                obj.url, obj.body)
        obj.meta[b"crawled_date"] = datetime.datetime.now()
        return obj
