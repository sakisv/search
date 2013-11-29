from bs4 import BeautifulSoup
from nltk.stem import SnowballStemmer
from nltk.stem.isri import ISRIStemmer
import re

def ensure_list(item):
    try:
        iter(item)
    except TypeError:  # not iterable ie. not list-like
        item = [item]
    return item


def clean_and_fix(text):
    '''
    For the given text:
     - remove html tags
     - remove punctuation
     - convert all whitespace to single space
    '''

    # remove html tags
    soup = BeautifulSoup(text, 'html.parser')
    text = soup.get_text()

    # remove punctuation
    text = re.sub(
        r'[!"#\$%&\'\(\)\*\+,\-\./\:;<=>\?@\[\\\]\^_`{|}~]+', '', text)

    # convert multiple whitespace to a single space
    text = re.sub('\s+', ' ', text)
    return text.lower()

def get_stemmer(languages, lang_code):
    '''
    Based on the lang_code, get the appropriate stemmer
    if no language is supplied, English is assumed
    '''
    lang = (languages[lang_code] if lang_code in languages.keys()
        else 'english')

    stemmer = SnowballStemmer(lang) if lang != 'arabic' else ISRIStemmer()

    return stemmer

