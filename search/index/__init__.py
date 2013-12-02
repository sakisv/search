from .stopwords import STOPWORDS
from ..util import ensure_list, clean_and_fix, get_stemmer


class Indexer(object):

    def __init__(self, redis):
        self.languages = {
            'en': 'english',
            'fr': 'french',
            'es': 'spanish',
            'ar': 'arabic'
        }

        self.fields = {
            'body': 1,
            'title': 1.5
        }

        # the field we will look for the document's id
        self.id = 'id'

        # redis connection to use
        self.redis = redis

    def create_reverse_index(self, documents, lang_code='en', fields=None,
            index_name='', id_field='', id_prefix='', id_suffix=''):
        '''
        Creates the reverse index for the list of documents passed as argument
        arguments:
        documents: a list of documents to parse
        lang_code: language code for to use for stemming, default: 'en'
        fields: a dict of fields and weights in the form of
            {'field_name': weight} default: {'body': 1, 'title': 1.5}
        index_name: the name of reverse index to be used
        id_field: the id field of the document. default: 'id'
        id_prefix: when specified, it will prefix the document's id
        id_suffix: when specified, it will suffix the document's id
        '''
        self.stemmer = get_stemmer(self.languages, lang_code)

        if fields:
            self.fields = fields

        if id_field:
            self.id = id_field

        # make sure even one document is parsed as list
        documents = ensure_list(documents)

        pipe = self.redis.pipeline()
        for document in documents:
            for field, weight in self.fields.iteritems():
                if hasattr(document, field):
                    item = getattr(document, field)
                    # fix that field
                    item = clean_and_fix(item)
                    collection = self._create_word_collection(item)
                    for word, score in collection.iteritems():
                        score *= weight  # update score based on the field's weight
                        key = 'index:%s:%s' % (index_name, word)
                        doc_id = '%s%s%s' % (id_prefix, getattr(document, self.id), id_suffix)

                        pipe.zadd(key, score, doc_id)

        pipe.execute()
        # if we get up to this point, everything should be fine
        return True

    def clean_index(self, prefix=None):
        '''
        Removes the keys matching that prefix from redis
        If no prefix is provided, performs a flushdb
        '''

        if prefix:
            keys = self.redis.keys('index:%s:*' % prefix)
            if keys:
                self.redis.delete(*keys)
        else:
            self.redis.flushdb()

    def _create_word_collection(self, document):
        '''
        Parse the document, create and return a collection of words
        and their scores
        '''
        words = document.split(' ')

        # if a word is not in STOPWORDS and is not too small, keep it's stem
        words = [self.stemmer.stem(x) for x in words if x > 2 and x not in STOPWORDS]
        count = len(words)

        # count the presence of each word
        collection = {}
        for w in words:
            collection[w] = (collection.get(w, 0)) + 1

        # frequency of each word
        for w in collection.keys():
            collection[w] /= float(count) * 10

        return collection
