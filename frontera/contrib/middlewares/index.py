from __future__ import absolute_import
from frontera.core.components import Middleware
from elasticsearch_dsl import DocType, Text, Date
from elasticsearch_dsl.connections import connections


class NewsArticle(DocType):
    title = Text(analyzer='snowball')
    text = Text(analyzer='snowball')
    url = Text(index='not_analyzed')
    published_date = Date()
    crawled_date = Date()

    class Meta:
        index = 'news'

    def save(self, ** kwargs):
        return super(NewsArticle, self).save(** kwargs)


class BaseIndexMiddleware(Middleware):
    component_name = 'Base Index Middleware'

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
        return self._add_to_index(response)

    def links_extracted(self, request, links):
        return request

    def request_error(self, request, error):
        return request

    def _add_to_index(self, obj):
        raise NotImplementedError


class ElasticSearchIndexMiddleware(BaseIndexMiddleware):
    """
    This :class:`Middleware <frontera.core.components.Middleware>` will add a 'text', 'description', 
    'published_date', 'crawled_date' field for every
    :attr:`Response.meta <frontera.core.models.Response.meta>` if is activated.
    """

    component_name = 'ElasticSearch Index Middleware'

    def __init__(self, manager):

        connections.create_connection(hosts=[manager.settings.get('ELASTICSEARCH_SERVER', "localhost")])
        NewsArticle.init()
    def _delete_fields(self, obj):
        del obj.meta[b"text"]
        del obj.meta[b"title"]
        try:
            del obj.meta[b"published_date"]
        except:
            pass
        del obj.meta[b"crawled_date"]
        

    def _add_to_index(self, obj):
        id = str(obj.meta[b'fingerprint'])
        article = NewsArticle(meta={'id': id})
        article.url = obj.url
        article.text = obj.meta[b"text"]
        article.title = obj.meta[b"title"]
        try:
            article.published_date = obj.meta[b"published_date"]
        except:
            pass
        article.crawled_date = obj.meta[b"crawled_date"]
        try:
            article.save()
        except:
            pass
        self._delete_fields(obj)
        return obj
