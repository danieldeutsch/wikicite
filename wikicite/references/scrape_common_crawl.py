import argparse
import bz2
import gzip
import json
import logging
import os
import requests
import sys
import time
from glob import glob
from io import BytesIO
from requests_futures.sessions import FuturesSession
from tqdm import tqdm
from typing import Any, Dict, Iterable, List, Set, Tuple
from warcio.archiveiterator import ArchiveIterator

from wikicite.references.indexed_bz2_file import IndexedBz2FileWriter

logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(module)s: %(message)s',)

_num_errors = 0
_max_errors = 20


def response_hook(response, *args, **kwargs):
    # 206 means the response has a partial response as dictated by the headers
    if response.status_code == 206:
        raw_data = BytesIO(response.content)
        for record in ArchiveIterator(raw_data):
            # There should only be 1 record
            try:
                html = record.content_stream().read().decode()
                response.data = html
            except UnicodeDecodeError:
                response.data = None
            return
    elif response.status_code == 429:
        print('429')
        sys.exit('429 status code')
    else:
        logging.warning(f'Non-206 response: {response.status_code}')
        response.data = None
        return


class Scraper(object):
    def __init__(self, num_workers: int, max_qps: float, window_length: float) -> None:
        self.num_workers = num_workers
        self.max_qps = max_qps
        self.window_length = window_length
        self.num_queries = 0
        self.start = time.time()
        self.session = FuturesSession(max_workers=num_workers)
        self.session.hooks['response'] = response_hook

    def _scrape_batch(self, requests: List[Tuple[str, str, int, int]]) -> Iterable[Tuple[str, Dict[str, str]]]:
        futures = []
        for url, request_url, offset, end_offset in requests:
            future = self.session.get(request_url, headers={'Range': f'bytes={offset}-{end_offset}'})
            futures.append(future)

        for (url, _, _, _), future in zip(requests, futures):
            try:
                response = future.result()
                if response.data is not None:
                    yield url, response.data
            except Exception as e:
                logging.error(f'Exception in request: {type(e)}')
                logging.error(f'Sleeping for 10 seconds')
                time.sleep(10)
                global _num_errors
                _num_errors += 1
                if _num_errors >= _max_errors:
                    logging.error('Hit maximum number of errors, exiting')
                    sys.exit('Hit maximum number of errors, exiting')

    def scrape(self, requests: Iterable[Tuple[str, str, int, int]]) -> Iterable[Tuple[str, Dict[str, str]]]:
        batch_size = self.num_workers
        batch = []
        for url, request_url, offset, end_offset in requests:
            batch.append((url, request_url, offset, end_offset))
            if len(batch) >= batch_size:
                queries_in_batch = len(batch)

                # Check if the window should be reset
                elapsed = time.time() - self.start
                if elapsed >= self.window_length:
                    self.start = time.time()
                    self.num_queries = 0

                # Compute the effective QPS after sending these requests and
                # sleep until it's below the maximum QPS
                num_queries = self.num_queries + queries_in_batch
                while (num_queries / (time.time() - self.start)) > self.max_qps:
                    time.sleep(0.1)

                # Now actually do the scraping
                for url, html in self._scrape_batch(batch):
                    yield url, html
                self.num_queries += queries_in_batch

                # Reset the batch for the next round
                batch.clear()

        # Clean up any leftover urls
        if batch:
            for url, html in self._scrape_batch(batch):
                yield url, html

    def qps(self) -> Tuple[float, float, float]:
        elapsed = time.time() - self.start
        qps = self.num_queries / elapsed
        return self.num_queries, elapsed, qps


def get_shard_id(locations_file_path: str) -> str:
    filename = os.path.basename(locations_file_path)
    hyphen_index = filename.find('-')
    period_index = filename.find('.')
    shard_id = filename[hyphen_index + 1:period_index]
    return shard_id


def load_locations(locations_file: str) -> List[Tuple[str, str, int, int]]:
    # Iterate over the directories in reversed order (reverse chronological) so
    # the more recent pages have higher priority (do we want this ordering?)
    logging.info(f'Loading locations from {locations_file}')
    locations = []
    with bz2.open(locations_file, 'rb') as f:
        for line in f:
            data = json.loads(line.decode())
            canonical_url = data['canonical_url']
            filename = data['filename']
            length = data['length']
            offset = data['offset']
            end_offset = offset + length - 1
            request_url = f'https://commoncrawl.s3.amazonaws.com/{filename}'
            locations.append((canonical_url, request_url, offset, end_offset))
        return locations


def main(args):
    locations_file_path = args.locations_file_path
    shard_id = get_shard_id(locations_file_path)

    output_dir = 'data/references/html'
    output_file = f'{output_dir}/html-{shard_id}.jsonl.bz2'
    output_index_file = f'{output_dir}/html-{shard_id}-index.jsonl.bz2'
    os.makedirs(output_dir, exist_ok=True)

    locations = load_locations(locations_file_path)
    scraper = Scraper(args.num_workers, args.max_qps, args.window_length)
    try:
        with IndexedBz2FileWriter(output_file, output_index_file) as out:
            found, total = 0, 0
            for i, (canonical_url, html) in enumerate(scraper.scrape(locations)):
                total += 1
                if html:
                    found += 1
                    output_data = {
                        'canonical_url': canonical_url,
                        'html': html
                    }
                    output_string = json.dumps(output_data) + '\n'
                    out.write(canonical_url, output_string)
                else:
                    out.write(canonical_url, None)

                if (i + 1) % 1000 == 0:
                    num_queries, elapsed, qps = scraper.qps()
                    successful = found / total * 100
                    logging.info(f'QPS: {qps:.2f}, Progress: {i + 1}, Successful: {found} / {total} = {successful:.2f}%')
            logging.info(f'Finishing crawling {locations_file_path}')
    except KeyboardInterrupt as e:
        logging.exception(e)
        logging.info('Terminating scraping')
    except Exception as e:
        logging.exception(e)
        logging.error('Terminating scraping due to exception')

    logging.info('Exiting')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('locations_file_path')
    argp.add_argument('--num-workers', type=int, default=10)
    argp.add_argument('--max-qps', type=int, default=10)
    argp.add_argument('--window-length', type=int, default=30)
    args = argp.parse_args()
    main(args)
