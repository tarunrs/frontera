from __future__ import absolute_import
from frontera.core.components import Middleware
from elasticsearch_dsl import DocType, Text, Date, Keyword
from elasticsearch_dsl.connections import connections


class NewsArticle(DocType):
    title = Text(analyzer='snowball')
    text = Text(analyzer='snowball')
    url = Keyword(index='not_analyzed')
    published_date = Date()
    crawled_date = Date()
    named_entities = Keyword(index='not_analyzed', multi=True)
    netloc = Keyword(index='not_analyzed')
    authors = Keyword(index='not_analyzed', multi=True)
    image = Keyword(index='not_analyzed')
    content_hash = Keyword(index='not_analyzed')
    stemmed_sentences = Keyword(index='not_analyzed', multi=True)


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

    component_name = 'ElasticSearch Index Middleware'

    def __init__(self, manager):

        connections.create_connection(hosts=[manager.settings.get('ELASTICSEARCH_SERVER', "localhost")])
        NewsArticle.init()

    def _delete_fields(self, obj):
        del obj.meta[b"text"]
        del obj.meta[b"content_hash"]
        del obj.meta[b"title"]
        del obj.meta[b"html"]
        try:
            del obj.meta[b"published_date"]
        except:
            pass
        del obj.meta[b"crawled_date"]
        try:
            del obj.meta[b"named_entities"]
        except:
            pass
        try:
            del obj.meta[b"stemmed_sentences"]
        except:
            pass
        try:
            del obj.meta[b"authors"]
        except:
            pass
        try:
            del obj.meta[b"image"]
        except:
            pass

    def add_to_index(self, obj):
        return self._add_to_index(obj)

    def _add_to_index(self, obj):
        id = str(obj.meta[b'fingerprint'])
        article = NewsArticle(meta={'id': id})
        article.url = obj.url
        article.text = obj.meta[b"text"]
        article.content_hash = obj.meta[b"content_hash"]
        article.title = obj.meta[b"title"]
        try:
            article.published_date = obj.meta[b"published_date"]
        except:
            pass
        article.crawled_date = obj.meta[b"crawled_date"]
        article.named_entities = obj.meta[b"named_entities"]
        article.stemmed_sentences = obj.meta[b"stemmed_sentences"]
        article.netloc = obj.meta[b"domain"][b'netloc']
        article.authors = obj.meta[b"authors"]
        article.image = obj.meta[b"image"]

        try:
            article.save()
        except:
            pass
        try:
            self._delete_fields(obj)
        except:
            pass
        return obj
