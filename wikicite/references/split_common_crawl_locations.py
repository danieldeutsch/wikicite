import bz2
import json
from glob import glob
from tqdm import tqdm

_urls_per_split = 50000


def main():
    index_dir = 'data/references/common-crawl-locations'

    locations = {}
    # Iterate over the data in sorted order to prioritize the most recent crawl
    for crawl_dir in tqdm(sorted(glob(f'{index_dir}/*')), desc='Reading locations'):
        for index_file_path in tqdm(glob(f'{crawl_dir}/*.bz2')):
            with bz2.open(index_file_path, 'rb') as f:
                for line in f:
                    data = json.loads(line.decode())
                    canonical_url = data['canonical_url']
                    locations[canonical_url] = line

    # Write all of the data to sharded files
    values = list(locations.values())
    for i, offset in enumerate(tqdm(range(0, len(values), _urls_per_split), desc='Writing locations')):
        with bz2.open(f'{index_dir}/locations-{i}.jsonl.bz2', 'wb') as out:
            for line in tqdm(values[offset:offset + _urls_per_split]):
                out.write(line)


if __name__ == '__main__':
    main()
