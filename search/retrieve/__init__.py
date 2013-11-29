from ..util import clean_and_fix

CONTENT_MAP = {
    'p': StaticPage,
    's': Page,
    'a': Page,
}


class Retriever(object):
    def __init__(self, redis):
        self.redis = redis

    def retrieve(self, query, indexes):

        # collect term indexed data from redis
        data = {}
        words = self._tokeniser(query)
        for word in words:
            key = 'index:*:%s' % word
            data[key] = self.redis.zrange(key, 0, -1, withscores=True)

        # process data to produce set of result documents
        union = {}
        for docs in data.values():
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

    def _tokeniser(self, query):
        # first fix and clean it
        query = clean_and_fix(query)
        return query.split(' ')
