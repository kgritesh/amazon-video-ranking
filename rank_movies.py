# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division

import json
import os
from collections import defaultdict
from functools import partial
from multiprocessing.pool import Pool

import itertools

import functools

import sys

import re
from robobrowser import RoboBrowser

TYPE = {
    'movie': 'Movie',
    'tv': 'TV%20Show'
}

GENRES = {
}

LANG = {
    'english': 'en-us',
    'hindi': 'hn-in'
}

BASE_URI = 'https://www.primevideo.com'
SEARCH_URI = '{}/search?query={{}}'.format(BASE_URI)
DETAIL_URL = '{}{{}}'.format(BASE_URI)

URL_MAP = {
    'default': 'p_n_entity_type%3D{type}',
    'language': "bq%253D%2528and%2Bsort%253A%2527featured-rank%2527%2B%2528and%2Bav_language_spoken%253A%2527IN%253A{lang}%2527%2Bentity_type%253A%2527{type}%2527%2529%2529",
    'genre': "bq%253D%2528and%2Bsort%253A%2527featured-rank%2527%2B%2528and%2Bentity_type%253A%2527Movie%2527%2Bav_primary_genre%253A%2527{genre}%2527%2529%2529",
    'language_genre': "bq%253D%2528and%2Bsort%253A%2527featured-rank%2527%2B%2528and%2Bentity_type%253A%2527Movie%2527%2Bav_primary_genre%253A%2527{genre}%2527%2Bav_language_spoken%253A%2527IN%253A{language}%2527%2529%2529"
}


def parse_page(b, rating_map):
    titles = [(s.get_text(), DETAIL_URL.format(s['href']))
              for s in b.find_all(class_='av-result-card-title')]
    ratings = [s.get_text() for s in b.find_all(class_='av-result-card-rating')]
    for title, rating in zip(titles, ratings):
        rating_map[rating].append(title)


def has_passed_last_page(b):
    if b.find(class_='av-refine-bar-applied-filter') or \
            b.find(text=re.compile('something went wrong ')):
        return True


def loop_pages(b, url, range_=None):
    from_, to_ = range_ or (0, None)
    to_ = to_ or sys.maxsize
    i = 0
    rating_map = defaultdict(list)
    while True:
        b.open(url + '&from={}'.format(from_))
        from_ += 20
        i += 1
        if from_ >= to_ or has_passed_last_page(b):
            break

        parse_page(b, rating_map)

    return rating_map


def combine_rating_map(x, y):
    for key, value in y.items():
        x[key].extend(value)
    return x


def get_partitions(size, num):
    chunks = range(0, size, int(size / num))
    return zip(chunks, itertools.chain(chunks[1:], [None]))


def get_ranking(url, user_agent, session_token):
    pool = Pool(processes=8, )
    b = RoboBrowser(user_agent=user_agent)
    b.session.cookies['session-token'] = session_token
    func = partial(loop_pages, b, url)

    results = pool.map(func, get_partitions(600, 10))
    print ("Fetched Rankings")
    combined_map = functools.reduce(combine_rating_map, results, defaultdict(list))
    rankings = sorted(combined_map.keys(), key=float, reverse=True)
    titles = [t for t in itertools.chain(*((combined_map[r], r) for r in rankings))]
    print(json.dumps(titles, indent=4))


def get_ranking_single(url, user_agent, session_token):
    b = RoboBrowser(user_agent=user_agent)
    b.session.cookies['session-token'] = session_token
    combined_map = loop_pages(b, url)
    rankings = sorted(combined_map.keys(), key=float, reverse=True)
    titles = [t for t in
              itertools.chain(*((combined_map[r], r) for r in rankings))]

    print(json.dumps(titles, indent=4))


def get_url(type_, lang=None, genre=None):
    type_ = TYPE[type_]
    if lang is not None and genre is not None:
        lang = LANG[lang]
        genre = GENRES[genre]        
        query = URL_MAP['language_genre'].format(lang=lang, type=type_, genre=genre)
    
    elif lang is not None:
        lang = LANG[lang]
        query = URL_MAP['language'].format(lang=lang, type=type_)
        
    elif genre is not None:
        genre = GENRES[genre]
        query = URL_MAP['genre'].format(genre=genre, type=type_)

    else:
        query = URL_MAP['default'].format(type=type_)

    return SEARCH_URI.format(query)
        

if __name__ == '__main__':
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    session_token = '"3mUznPMBV4K+0DIa19nTEMnEe7A03qgLsoKS152MtjElqQa1KMWGF60MIF1AcCLjwfOeKA0qrddr54UguB2ZQ4O/02GL1jx7Ow8lZdk4D3x3y+EQv9a4poWwBgkJ7R07v8O8N5xHMcmMIL6HfONfqrn/s15dZWcmhlsn9Zoiax0FzZEgryhrdu9mPRI4G4GhOb09bfyGfYWeJdd8TMoaiXWkyRWmsdV4oDyySW8a/5CmrxkRNHiRDbn+L2bTybafd0pSh0bFKMUlEjMnNpzDBw==";'
    url = get_url(type_='movie')
    print(url)
    get_ranking(url, user_agent, session_token)
