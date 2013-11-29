from ..util import clean_and_fix

CONTENT_MAP = {
    'p': StaticPage,
    's': Page,
    'a': Page,
}

class Search(object):
    def __init__(self, redis):
        self.redis = redis

    def search(self, query):
        context = {'q': query}

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
        ranked = sorted(union,
            key=lambda x: union[x]['score'] * union[x]['count'], reverse=True)

        # retrieve results from redis
        results = []
        for page_key in ranked:
            page_id, lang = page_key.split('_')
            page = CONTENT_MAP[page_id[0]]('', page_id, lang)
            try:
                page.retrieve()
            except BadUrlForContent:
                pass
            except NoSuchContent:
                continue
            results.append(page.content)
        context['results'] = results
        return render(request, 'search.html', context)


    def _tokeniser(self, query):
        # first fix and clean it
        query = clean_and_fix(query)
        return query.split(' ')
