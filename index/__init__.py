from nltk.stem import SnowballStemmer
from nltk.stem.isri import ISRIStemmer
from bs4 import BeautifulSoup
import re


class Indexer(object):

    def __init__(self):
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
        
        self.id = 'id'

    def create_reverse_index(self, documents, lang_code='en', fields=None, prefix='', 
            id_field=''):
        '''
        Creates the reverse for the list of document passed as argument
        arguments:
        documents: a list of documents to parse
        lang_code: language code for to use for stemming, default: 'en'
        fields: a dict of fields in the form of {'field_name': weight}
            default: {'body': 1, 'title': 1.5}
        prefix: the prefix of reverse index to be used
        id_field: the id field of the document. default: 'id'
        '''
        self.stemmer = self._get_stemmer(lang_code)

        if fields:
            self.fields = fields

        documents = self._clean_and_fix(documents)
        
        for document in documents:
            for field, value in self.fields.iteritems(): 
                if hasattr(document, field):


    def _get_stemmer(self, lang_code):
        '''
        Based on the lang_code, get the appropriate stemmer
        if no language is supplied, English is assumed
        '''
        lang = (self.languages[lang_code] if lang_code in self.languages.keys()
            else 'english')

        stemmer = SnowballStemmer(lang) if lang != 'arabic' else ISRIStemmer()

        return stemmer

    def _clean_and_fix(self, documents):
        '''
        For each field in each document:
         - remove html tags
         - remove punctuation
         - convert all whitespace to single space
        '''
        for document in documents:
            for field in self.fields:
                if hasattr(document, field):
                    text = getattr(document, field)

                    # remove html tags
                    soup = BeautifulSoup(text, 'html.parser')
                    text = soup.get_text()

                    # remove punctuation
                    text = re.sub(
                        r'[!"#\$%&\'\(\)\*\+,\-\./\:;<=>\?@\[\\\]\^_`{|}~]+', '', text)

                    # convert multiple whitespace to a single space
                    text = re.sub('\s+', ' ', text)

                    # update object with the clean text
                    setattr(document, field, text)
        return documents
