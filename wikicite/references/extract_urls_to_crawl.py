import argparse
import bz2
import json
import logging
import os
import lxml.html
import requests
import sys
import urllib.parse
import urllib.request
from collections import Counter
from io import StringIO
from typing import Dict, List, Set, Tuple

timeout = 60
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

_blacklisted_domains = set([
    'books.google.com',
    'books.google.co.in',
    'youtube.com'
])

_blacklisted_extensions = set([
    '.pdf', '.jpeg', '.jpg', '.png', '.mp3', '.mp4',
    '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx'
])


def load_living_people(file_path: str) -> Set[int]:
    living_people = set()
    with bz2.open(file_path, 'rb') as f:
        for line in f:
            data = json.loads(line.decode())
            if 'Living people' in data['categories']:
                page_id = data['page_id']
                living_people.add(page_id)
    return living_people


def get_url_to_scrape(metadata: Dict[str, str]) -> str:
    if 'url' in metadata:
        url = metadata['url']
        if url.startswith('https://web.archive.org/web'):
            url = url[len('https://web.archive.org/web/20080628063306/'):]
        return url
    return None


def is_scrapable(url: str) -> bool:
    # First check to see if the domain is blacklisted
    try:
        parse = urllib.parse.urlparse(url)
        domain = parse.netloc
        if domain.startswith('www.'):
            domain = domain[4:]

        if domain in _blacklisted_domains:
            return False

        # Check to make sure that the extension (if it exists) isn't blacklisted
        extension = os.path.splitext(parse.path)[1]
        if extension in _blacklisted_extensions:
            return False
        return True
    except ValueError:
        return False


def main(args):
    shard_id = args.shard_id

    output_dir = 'data/references/urls'
    output_file = os.path.join(output_dir, f'urls-{shard_id}.jsonl.bz2')
    os.makedirs(output_dir, exist_ok=True)

    # Load the list of living people
    category_file = f'data/wikipedia/categories/categories-{shard_id}.jsonl.bz2'
    living_people = load_living_people(category_file)

    # Load all of the references which need to be scraped
    article_file = f'data/wikipedia/articles/articles-{shard_id}.jsonl.bz2'
    urls_to_scrape = set()
    with bz2.open(article_file, 'rb') as f:
        for line in f:
            article = json.loads(line.decode())
            title = article['title']
            page_id = article['page_id']
            if page_id in living_people:
                for reference in article['references'].values():
                    url = get_url_to_scrape(reference)
                    if url and is_scrapable(url):
                        urls_to_scrape.add(url)

    with bz2.open(output_file, 'w') as out:
        for url in urls_to_scrape:
            data = {'url': url}
            out.write(json.dumps(data).encode() + b'\n')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('shard_id', type=int)
    args = argp.parse_args()
    main(args)
