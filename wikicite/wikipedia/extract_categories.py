import argparse
import bz2file as bz2
import json
import logging
import lxml.html
import os
import re
import sys
from io import StringIO

from wikicite.document import parse_document
from wikicite.reference import parse_references

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

_wikitext_regex = re.compile('\[\[Category:(.+?)\]\]')


def main(args):
    shard_id = args.shard_id

    output_dir = 'data/wikipedia/categories'
    output_file = os.path.join(output_dir, f'categories-{shard_id}.jsonl.bz2')
    os.makedirs(output_dir, exist_ok=True)

    wikitext_file = f'data/wikipedia/wikitext/wikitext-{shard_id}.jsonl.bz2'
    with bz2.open(wikitext_file, 'rb') as f:
        with bz2.open(output_file, 'wb') as out:
            count = 0
            logging.info('Starting to parse')
            for line in f:
                data = json.loads(line.decode())
                title = data['title']
                page_id = data['page_id']
                wikitext = data['wikitext']

                categories = []
                for match in _wikitext_regex.finditer(wikitext):
                    category = match.group(1).strip()
                    categories.append(category)

                output_data = {
                    'title': title,
                    'page_id': page_id,
                    'categories': categories
                }
                out.write(json.dumps(output_data).encode() + b'\n')

    logging.info('Terminating')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('shard_id', type=int)
    args = argp.parse_args()
    main(args)
