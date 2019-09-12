import argparse
import bz2file as bz2
import json
import logging
import nltk
import os
import subprocess
import sys
from joblib import Parallel, delayed
from typing import Iterable, List, Tuple
from unidecode import unidecode

from wikicite.references.indexed_bz2_file import IndexedBz2FileWriter

timeout = 60
logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(module)s: %(message)s',)


def get_shard_id(html_file_path: str) -> str:
    filename = os.path.basename(html_file_path)
    hyphen_index = filename.find('-')
    period_index = filename.find('.')
    shard_id = filename[hyphen_index + 1:period_index]
    return shard_id


def parse_html(url: str, html: str) -> str:
    if not os.path.exists('ext/unfluff/bin/unfluff'):
        raise Exception('unfluff not installed. Run "sh scripts/setup.sh"')

    devnull = open(os.devnull, 'w')
    process = subprocess.Popen('node ext/unfluff/bin/unfluff'.split(),
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=devnull)
    try:
        stdout, _ = process.communicate(input=html.encode(), timeout=timeout)
        data = json.loads(stdout.decode())

        text = data['text']
        paragraph_texts = text.split('\n\n')
        paragraphs = []
        for paragraph in paragraph_texts:
            paragraph = unidecode(paragraph)
            sentences = nltk.sent_tokenize(paragraph)
            sentences = list(map(lambda s: s.strip(), filter(None, sentences)))
            if len(sentences) > 0:
                paragraphs.append(sentences)
        data['paragraphs'] = paragraphs
        return url, data
    except (subprocess.TimeoutExpired, json.decoder.JSONDecodeError):
        process.kill()
        return url, None


def data_generator(file_path: str, batch_size: int) -> Iterable[List[Tuple[str, str]]]:
    batch = []
    with bz2.open(file_path, 'rb') as f:
        for line in f:
            data = json.loads(line.decode())
            canonical_url = data['canonical_url']
            html = data['html']
            batch.append((canonical_url, html))
            if len(batch) == batch_size:
                yield batch
                batch.clear()

    if batch:
        yield batch


def main(args):
    shard_id = get_shard_id(args.html_file_path)

    output_dir = 'data/references/documents'
    output_file = os.path.join(output_dir, f'documents-{shard_id}.jsonl.bz2')
    output_index_file = os.path.join(output_dir, f'documents-{shard_id}-index.jsonl.bz2')
    os.makedirs(output_dir, exist_ok=True)

    with IndexedBz2FileWriter(output_file, output_index_file) as out:
        count = 0
        logging.info('Starting to parse')
        with Parallel(n_jobs=args.num_cores) as parallel:
            for batch in data_generator(args.html_file_path, args.batch_size):
                jobs = [delayed(parse_html)(url, html) for url, html in batch]
                for canonical_url, data in parallel(jobs):
                    if data is None or len(data) == 0:
                        logging.warn(f'Extracting body text from {canonical_url} returned None')
                        continue

                    output_data = {
                        'canonical_url': canonical_url,
                        **data
                    }
                    output_string = json.dumps(output_data) + '\n'
                    out.write(canonical_url, output_string)

                    count += 1
                    if count % 1000 == 0:
                        logging.info(f'Processed {count} entries')

    logging.info('Terminating')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('html_file_path')
    argp.add_argument('--num-cores', type=int, default=1)
    argp.add_argument('--batch-size', type=int, default=100)
    args = argp.parse_args()
    main(args)
