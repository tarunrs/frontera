from __future__ import absolute_import
from frontera.core.components import Middleware
from elasticsearch_dsl import DocType, Text, Date, Keyword
from elasticsearch_dsl.connections import connections
from collections import defaultdict


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
    location = Keyword(index='not_analyzed', multi=True)
    language = Keyword(index='not_analyzed')
    ne_person = Keyword(index='not_analyzed', multi=True)
    ne_norp = Keyword(index='not_analyzed', multi=True)
    ne_facility = Keyword(index='not_analyzed', multi=True)
    ne_org = Keyword(index='not_analyzed', multi=True)
    ne_gpe = Keyword(index='not_analyzed', multi=True)
    ne_loc = Keyword(index='not_analyzed', multi=True)
    ne_product = Keyword(index='not_analyzed', multi=True)
    ne_event = Keyword(index='not_analyzed', multi=True)
    ne_work_of_art = Keyword(index='not_analyzed', multi=True)
    ne_law = Keyword(index='not_analyzed', multi=True)
    ne_language = Keyword(index='not_analyzed', multi=True)
    ne_date = Keyword(index='not_analyzed', multi=True)
    ne_time = Keyword(index='not_analyzed', multi=True)
    ne_percent = Keyword(index='not_analyzed', multi=True)
    ne_money = Keyword(index='not_analyzed', multi=True)
    ne_quantity = Keyword(index='not_analyzed', multi=True)
    ne_ordinal = Keyword(index='not_analyzed', multi=True)
    ne_cardinal = Keyword(index='not_analyzed', multi=True)
    crawl_source = Keyword(index='not_analyzed')

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

        connections.create_connection(hosts=manager.settings.get(
            'ELASTICSEARCH_SERVER', ["localhost"]))
        NewsArticle.init()

    def _delete_fields(self, obj):
        del obj.meta[b"text"]
        del obj.meta[b"content_hash"]
        del obj.meta[b"title"]
        del obj.meta[b"html"]
        del obj.meta[b"language"]
        del obj.meta[b"location"]
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

    def add_named_entities(self, article, named_entities):
        if not named_entities:
            return
        if type(named_entities) == list:
            article.named_entities = named_entities
        if type(named_entities) == defaultdict:
            exclude_ne_type = ["LANGUAGE", "DATE", "TIME",
                               "PERCENT", "MONEY", "QUANTITY", "ORDINAL", "CARDINAL"]
            all_entities = []
            for ne_type in named_entities:
                if ne_type == "PERSON":
                    article.ne_person = named_entities[ne_type]
                elif ne_type == "NORP":
                    article.ne_norp = named_entities[ne_type]
                elif ne_type == "FACILITY":
                    article.ne_facility = named_entities[ne_type]
                elif ne_type == "ORG":
                    article.ne_org = named_entities[ne_type]
                elif ne_type == "GPE":
                    article.ne_gpe = named_entities[ne_type]
                elif ne_type == "LOC":
                    article.ne_loc = named_entities[ne_type]
                elif ne_type == "PRODUCT":
                    article.ne_product = named_entities[ne_type]
                elif ne_type == "EVENT":
                    article.ne_event = named_entities[ne_type]
                elif ne_type == "WORK_OF_ART":
                    article.ne_work_of_art = named_entities[ne_type]
                elif ne_type == "LAW":
                    article.ne_law = named_entities[ne_type]
                elif ne_type == "LANGUAGE":
                    article.ne_language = named_entities[ne_type]
                elif ne_type == "DATE":
                    article.ne_date = named_entities[ne_type]
                elif ne_type == "TIME":
                    article.ne_time = named_entities[ne_type]
                elif ne_type == "PERCENT":
                    article.ne_percent = named_entities[ne_type]
                elif ne_type == "MONEY":
                    article.ne_money = named_entities[ne_type]
                elif ne_type == "QUANTITY":
                    article.ne_quantity = named_entities[ne_type]
                elif ne_type == "ORDINAL":
                    article.ne_ordinal = named_entities[ne_type]
                elif ne_type == "CARDINAL":
                    article.ne_cardinal = named_entities[ne_type]
                if ne_type not in exclude_ne_type:
                    all_entities += named_entities[ne_type]
            article.named_entities = list(set(all_entities))

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
        article.stemmed_sentences = obj.meta[b"stemmed_sentences"]
        article.netloc = obj.meta[b"domain"][b'netloc']
        article.authors = obj.meta[b"authors"]
        article.image = obj.meta[b"image"]
        article.location = obj.meta[b"location"]
        article.language = obj.meta[b"language"]
        try:
            self.add_named_entities(article, obj.meta[b"named_entities"])
        except Exception as e:
            print str(e)
        try:
            article.save()
        except Exception as e:
            print str(e)
        try:
            self._delete_fields(obj)
        except:
            pass
        return obj
