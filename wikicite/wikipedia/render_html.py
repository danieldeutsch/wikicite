import argparse
import bz2file as bz2
import json
import logging
import os
import subprocess
import sys

timeout = 60
logging.basicConfig(stream=sys.stderr, level=logging.INFO)


def render(wikitext: str) -> str:
    if not os.path.exists('ext/parsoid/bin/parse.js'):
        raise Exception('Parsoid is not installed. Run "sh scripts/setup.sh"')

    devnull = open(os.devnull, 'w')
    process = subprocess.Popen('node ext/parsoid/bin/parse --offline true'.split(),
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=devnull)
    try:
        stdout, _ = process.communicate(input=wikitext.encode(),
                                        timeout=timeout)
        return stdout.decode()
    except subprocess.TimeoutExpired:
        process.kill()
        return None


def main(args):
    shard_id = args.shard_id

    output_dir = 'data/wikipedia/html'
    output_file = os.path.join(output_dir, f'html-{shard_id}.jsonl.bz2')
    os.makedirs(output_dir, exist_ok=True)

    wikitext_file = os.path.join(f'data/wikipedia/wikitext/wikitext-{shard_id}.jsonl.bz2')
    with bz2.open(wikitext_file, 'rb') as f:
        with bz2.open(output_file, 'wb') as out:
            count = 0
            logging.info('Starting to render')
            for line in f:
                data = json.loads(line.decode())
                title = data['title']
                page_id = data['page_id']
                wikitext = data['wikitext']
                if wikitext is None:
                    logging.warn(f'Wikitext for ({title}, {page_id}) is `None`')
                    continue

                html = render(wikitext)
                if html is None:
                    logging.warn(f'Rendering ({title}, {page_id}) timed out')
                    continue

                output_data = {
                    'title': title,
                    'page_id': page_id,
                    'html': html
                }
                out.write(json.dumps(output_data).encode() + b'\n')

                count += 1
                if count % 1000 == 0:
                    logging.info(f'Processed {count} entries')

    logging.info('Terminating')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('shard_id', type=int)
    args = argp.parse_args()
    main(args)
