from __future__ import absolute_import
from frontera.core.components import Middleware
import articleDateExtractor
from frontera.utils.misc import get_crc32
import datetime
import nltk
from newspaper import Article


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
        return self._add_details(response)

    def links_extracted(self, request, links):
        return request

    def request_error(self, request, error):
        return request

    def _add_details(self, obj):
        raise NotImplementedError


class NewsDetailsExtractMiddleware(BaseExtractMiddleware):
    """
    This :class:`Middleware <frontera.core.components.Middleware>` will add a 'text', 'description', 
    'published_date', 'crawled_date' field for every
    :attr:`Response.meta <frontera.core.models.Response.meta>` if is activated.
    """

    component_name = 'News Details Extract Middleware'

    def add_details(self, obj, html):
        a = Article(obj.url)
        a.download(html=html)
        a.parse()
        obj.meta[b"text"] = a.text
        obj.meta[b"content_hash"] = get_crc32(a.text) 
        obj.meta[b"title"] = a.title
        obj.meta[b"html"] = a.html
        obj.meta[b"published_date"] = a.publish_date
        if obj.meta[b"published_date"] is None:
            obj.meta[b"published_date"] = articleDateExtractor.extractArticlePublishedDate(
                obj.url, obj.body)
        obj.meta[b"crawled_date"] = datetime.datetime.now()
        obj.meta[b"image"] = a.top_image
        obj.meta[b"authors"] = a.authors

        return obj

    def _add_details(self, obj):
        return self.add_details(obj, obj.body)


class EntityDetailsExtractMiddleware(BaseExtractMiddleware):
    """
    This :class:`Middleware <frontera.core.components.Middleware>` will add a 'text', 'description', 
    'published_date', 'crawled_date' field for every
    :attr:`Response.meta <frontera.core.models.Response.meta>` if is activated.
    """

    component_name = 'Entity Details Extract Middleware'

    def _extract_entity_names(self, t):
        entity_names = []

        if hasattr(t, 'label') and t.label:
            if t.label() == 'NE':
                entity_names.append(' '.join([child[0] for child in t]))
            else:
                for child in t:
                    entity_names.extend(self._extract_entity_names(child))

        return entity_names
    def add_details(self, obj):
        return self._add_details(obj)

    def _add_details(self, obj):
        sentences = nltk.sent_tokenize(obj.meta[b"text"])
        tokenized_sentences = [nltk.word_tokenize(
            sentence) for sentence in sentences]
        tagged_sentences = [nltk.pos_tag(sentence)
                            for sentence in tokenized_sentences]
        chunked_sentences = nltk.ne_chunk_sents(tagged_sentences, binary=True)
        entity_names = []
        for tree in chunked_sentences:
            entity_names.extend(self._extract_entity_names(tree))
        entity_names = [entity.lower() for entity in entity_names]
        entity_names = list(set(entity_names))
        obj.meta[b"named_entities"] = entity_names
        return obj
