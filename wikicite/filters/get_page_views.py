import argparse
import gzip
import json
import requests
import time
from tqdm import tqdm


def main(args):
    pages = set()
    with gzip.open(args.input_jsonl, 'rb') as f:
        for line in tqdm(f, desc='Loading input'):
            data = json.loads(line)
            page_title = data['page_title']
            page_id = data['page_id']
            pages.add((page_title, page_id))

    with gzip.open(args.output_jsonl, 'wb') as out:
        for page_title, page_id in tqdm(sorted(pages), desc='Getting page views'):
            try:
                url = f'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/{page_title}/monthly/20151010/20181231'
                response = requests.get(url)
                if response.status_code == 200:
                    content = json.loads(response.content.decode())
                    views = sum(item['views'] for item in content['items'])
                    output = {
                        'page_title': page_title,
                        'page_id': page_id,
                        'views': views
                    }
                    out.write(json.dumps(output).encode() + b'\n')
            except RuntimeError:
                pass

            time.sleep(0.1)


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('input_jsonl')
    argp.add_argument('output_jsonl')
    args = argp.parse_args()
    main(args)
