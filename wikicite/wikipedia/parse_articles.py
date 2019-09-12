import argparse
import bz2file as bz2
import json
import logging
import lxml.html
import os
import spacy
import sys
from io import StringIO

from wikicite.wikipedia.article import parse_article
from wikicite.wikipedia.reference import parse_references

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

nlp = spacy.load('en', disable=['ner'])


def main(args):
    shard_id = args.shard_id

    output_dir = 'data/wikipedia/articles'
    output_file = os.path.join(output_dir, f'articles-{shard_id}.jsonl.bz2')
    os.makedirs(output_dir, exist_ok=True)

    html_file = f'data/wikipedia/html/html-{shard_id}.jsonl.bz2'
    with bz2.open(html_file, 'rb') as f:
        with bz2.open(output_file, 'wb') as out:
            count = 0
            logging.info('Starting to parse')
            for line in f:
                data = json.loads(line.decode())
                title = data['title']
                page_id = data['page_id']
                html = data['html']

                if html is None:
                    logging.warn(f'HTML for ({title}, {page_id}) is `None`')
                    continue

                try:
                    tree = lxml.html.parse(StringIO(html))
                    lxml.etree.strip_tags(tree, lxml.etree.Comment)

                    references = parse_references(tree)
                    if references is None or len(references.references) == 0:
                        logging.warn(f'References for ({title}, {page_id}) are empty')
                        continue

                    article = parse_article(nlp, title, page_id, tree)
                    if article is None or article.is_empty():
                        logging.warn(f'Article for ({title}, {page_id}) is empty')
                        continue

                    output_data = article.to_json()
                    output_data['references'] = references.to_json()
                    out.write(json.dumps(output_data).encode() + b'\n')
                except Exception as e:
                    logging.warn(f'Exception processing ({title}, {page_id}). Exception: {e}')

                count += 1
                if count % 1000 == 0:
                    logging.info(f'Processed {count} entries')

    logging.info('Terminating')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('shard_id', type=int)
    args = argp.parse_args()
    main(args)
