import argparse
import bz2
import gzip
import json
import os
from glob import glob
from pywb.utils.canonicalize import UrlCanonicalizeException, canonicalize
from tqdm import tqdm
from typing import Dict, List, Set, Tuple


def get_shard_id(index_file_path: str) -> str:
    # '/path/to/cdx-00123.gz' -> 00123
    filename = os.path.basename(index_file_path)
    return filename[4:9]


def parse_index_entry(line: str) -> Dict[str, str]:
    first_space = line.index(' ')
    line = line[first_space + 1:]

    second_space = line.index(' ')
    json_string = line[second_space + 1:]
    data = json.loads(json_string)
    return {
        'status': int(data['status']),
        'filename': data['filename'],
        'length': int(data['length']),
        'offset': int(data['offset'])
    }


def load_urls(input_dir: str) -> Set[str]:
    urls = set()
    for file_path in tqdm(glob(f'{input_dir}/*.bz2')):
        with bz2.open(file_path, 'rb') as f:
            for line in f:
                data = json.loads(line.decode())
                url = data['url']
                try:
                    canonical_url = canonicalize(url)
                    urls.add(canonical_url)
                except UrlCanonicalizeException:
                    pass
    return urls


def main(args):
    shard_id = get_shard_id(args.index_file_path)

    output_dir = f'data/references/common-crawl-locations/{args.index_name}'
    output_file = f'{output_dir}/locations-{shard_id}.jsonl.bz2'
    os.makedirs(output_dir, exist_ok=True)

    input_dir = f'data/references/urls'
    urls = load_urls(input_dir)

    with bz2.open(output_file, 'w') as out:
        with gzip.open(args.index_file_path, 'rb') as f:
            for line in tqdm(f):
                line = line.decode().strip()
                first_space = line.index(' ')
                canonical_url = line[:first_space]

                if canonical_url in urls:
                    entry = parse_index_entry(line)
                    if entry['status'] != 200:
                        continue

                    output_data = {
                        'canonical_url': canonical_url,
                        'filename': entry['filename'],
                        'length': entry['length'],
                        'offset': entry['offset']
                    }
                    out.write(json.dumps(output_data).encode() + b'\n')

                    # We only want to process each url once. There might be
                    # multiple entries for the same canonical url
                    urls.remove(canonical_url)


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('index_name')
    argp.add_argument('index_file_path')
    args = argp.parse_args()
    main(args)
