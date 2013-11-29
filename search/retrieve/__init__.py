from ..util import clean_and_fix, ensure_list, get_stemmer


class Retriever(object):
    def __init__(self, redis):
        self.redis = redis

        self.languages = {
            'en': 'english',
            'fr': 'french',
            'es': 'spanish',
            'ar': 'arabic'
        }

    def retrieve(self, query, indexes, lang_code):
        # collect term indexed data from redis
        data = {}
        words = self._tokeniser(query, lang_code)

        # build a dict in the form of:
        # data = {
        #     'word1': {
        #         'index1': doc_list,
        #         'index2': doc_list
        #      },
        #     'word2': {
        #         'index1': doc_list,
        #         'index2': doc_list
        #      }
        # }
        # it's easier to have words than indexes as keys in the data dict,
        # because it's easier to get the list of documents for each word in each index
        indexes = ensure_list(indexes)
        for word in words:
            data[word] = {}
            for index in indexes:
                key = 'index:%s:%s' % (index, word)
                data[word][index] = self.redis.zrange(key, 0, -1, withscores=True)

        # process data to produce set of result documents
        union = {}
        for word, index_dict in data.iteritems():
            for docs in index_dict.values():
                for doc_id, score in docs:
                    if doc_id not in union:  # initialising
                        union[doc_id] = {}
                        union[doc_id]['score'] = score
                        union[doc_id]['count'] = 1
                    else:  # aggregating
                        union[doc_id]['score'] += score
                        union[doc_id]['count'] += 1

        # rank result documents
        ranked_keys = sorted(union,
            key=lambda x: union[x]['score'] * union[x]['count'], reverse=True)

        return ranked_keys

    def _tokeniser(self, query, lang_code):
        # first fix and clean it
        query = clean_and_fix(query)
        words = query.split(' ')
        stemmer = get_stemmer(self.languages, lang_code)
        return [stemmer.stem(x) for x in words]
