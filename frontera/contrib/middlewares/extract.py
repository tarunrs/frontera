from __future__ import absolute_import
from frontera.core.components import Middleware
from boilerpipe.extract import Extractor
from frontera.utils.misc import get_crc32
import datetime
import nltk
from newspaper import Article
from nltk.stem.snowball import SnowballStemmer
import pickle
import spacy
from collections import defaultdict


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

    def __init__(self, manager):
        self.language_map = pickle.load(
            open(manager.settings.get('LANGUAGES_FILE')))
        self.locations_map = pickle.load(
            open(manager.settings.get('LOCATIONS_FILE')))

    def get_language(self, domain):
        try:
            for el in self.language_map:
                if el in domain:
                    return self.language_map[el]
            return "en"
        except:
            return "en"

    def get_locations(self, domain):
        try:
            return self.locations_map[domain]
        except Exception as e:
            return ['undefined']

    def add_details(self, obj, html):
        domain = obj.meta[b"domain"][b'netloc']
        language = self.get_language(domain)
        location = self.get_locations(domain)
        a = Article(obj.url, language=language)
        if html is not None and len(html.strip()) == 0:
            html = None
        a.download(html=html)
        a.parse()
        if a.html is None or (a.html is not None and len(a.html) == 0):
            extractor = Extractor(extractor='ArticleExtractor', url=obj.url)
        else:
            extractor = Extractor(extractor='ArticleExtractor', html=a.html)
        obj.meta[b"text"] = extractor.getText()
        if len(a.text) > len(obj.meta[b"text"]):
            obj.meta[b"text"] = a.text
        obj.meta[b"content_hash"] = get_crc32(obj.meta[b"text"])
        obj.meta[b"title"] = a.title
        obj.meta[b"html"] = a.html
        obj.meta[b"published_date"] = a.publish_date
        obj.meta[b"crawled_date"] = datetime.datetime.now()
        obj.meta[b"image"] = a.top_image
        obj.meta[b"authors"] = a.authors
        obj.meta[b"language"] = language
        obj.meta[b"location"] = location
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
    stemmer = SnowballStemmer("english")
    s_nlp = spacy.load("en_core_web_sm")

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

    def stem_sentence(self, sentence):
        tokens = [self.stemmer.stem(token.lower()) for token in sentence]
        return " ".join(tokens)

    def stem_sentences(self, sentences):
        ret_sentences = []
        for sentence in sentences:
            ret_sentences.append(self.stem_sentence(sentence))
        return ret_sentences

    def extract_named_entities_using_spacy(self, sentences):
        nes = defaultdict(list)
        for s in sentences:
            if type(s) == str:
                # Ignore errors even if the string is not proper UTF-8 or has
                # broken marker bytes.
                # Python built-in function unicode() can do this.
                s = unicode(s, "utf-8", errors="ignore")
            else:
                # Assume the value object has proper __unicode__() method
                s = unicode(s)
            doc = self.s_nlp(s)
            spans = list(doc.ents) + list(doc.noun_chunks)
            for span in spans:
                span.merge()
            for e in doc.ents:
                if e.text.isupper():
                    nes[e.label_].append(e.text.strip())
                else:
                    nes[e.label_].append(e.text.lower().strip())
            for key in nes:
                nes[key] = list(set(nes[key]))
        return nes

    def _add_details(self, obj):
        sentences = nltk.sent_tokenize(obj.meta[b"text"])
        tokenized_sentences = [nltk.word_tokenize(
            sentence) for sentence in sentences]
        stemmed_sentences = self.stem_sentences(tokenized_sentences)
        if obj.meta[b"language"] == "en":
            obj.meta[b"named_entities"] = self.extract_named_entities_using_spacy(
                sentences)
        else:
            tagged_sentences = [nltk.pos_tag(sentence)
                                for sentence in tokenized_sentences]
            chunked_sentences = nltk.ne_chunk_sents(
                tagged_sentences, binary=True)
            entity_names = []
            for tree in chunked_sentences:
                entity_names.extend(self._extract_entity_names(tree))
            entity_names = [entity.lower() for entity in entity_names]
            entity_names = list(set(entity_names))
            obj.meta[b"named_entities"] = entity_names
        obj.meta[b"stemmed_sentences"] = stemmed_sentences
        return obj
