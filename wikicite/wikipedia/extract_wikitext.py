import argparse
import bz2file as bz2
import json
import logging
import os
import sys
from io import BytesIO
from lxml import etree
from typing import List, Optional, T, Tuple

_multistream_file = 'data/wikipedia/xml/enwiki-20190101-pages-articles-multistream.xml.bz2'
_index_file = 'data/wikipedia/xml/enwiki-20190101-pages-articles-multistream-index.txt.bz2'

logging.basicConfig(stream=sys.stderr, level=logging.INFO)


def load_offsets_from_index(index_file_path: str) -> List[Tuple[int, int]]:
    logging.info(f'Loading byte offsets from {index_file_path}')
    pairs = []
    with bz2.open(index_file_path, 'r') as f:
        current = None
        for line in f:
            index = line.index(b':')
            offset = int(line[:index])
            if current != offset:
                if current is not None:
                    pairs.append((current, offset))
                current = offset

        # The last item will not have an ending pair
        if current is not None:
            pairs.append((current, None))

    logging.info(f'Loaded {len(pairs)} pairs')
    return pairs


def shard_data(data: List[T],
               num_shards: int,
               shard_id: int) -> List[T]:
    shard_data = []
    for i in range(shard_id, len(data), num_shards):
        shard_data.append(data[i])
    return shard_data


def load_wikitext_from_multistream(multistream_file_path: str,
                                   start: int,
                                   end: Optional[int] = None) -> List[str]:
    with open(multistream_file_path, 'rb') as byte_f:
        byte_f.seek(start, 0)
        if end is not None:
            # Since we know what chunk we need to process, read the entire
            # block into memory
            num_bytes = end - start
            compressed_bytes = byte_f.read(num_bytes)
            stream_bytes = bz2.open(BytesIO(compressed_bytes), 'rb').read()
        else:
            # The end of the file will also contain an extra </mediawiki> tag
            # that will mess up the parsing unless it's removed
            stream_bytes = bz2.open(byte_f, 'rb').read()
            stream_bytes = stream_bytes[:-len(b'</mediawiki>')]

        # The `stream_bytes` contain multiple <page> tags, which serve as the root.
        # I don't know if the lxml library can parse multiple xmls within
        # the same byte stream. A hacky fix is to wrap the xmls in a new tag
        # so the library will parse it correctly.
        stream_bytes = b'<pages>\n' + stream_bytes + b'</pages>'
        wikitexts = []
        for _, page in etree.iterparse(BytesIO(stream_bytes), tag='page'):
            title = page.xpath('title')[0].text
            page_id = int(page.xpath('id')[0].text)
            wikitext = page.xpath('revision/text')
            wikitexts.append((title, page_id, wikitext[0].text))
        return wikitexts


def main(args):
    shard_id = args.shard_id
    num_shards = args.num_shards

    offset_pairs = load_offsets_from_index(_index_file)
    shard_pairs = shard_data(offset_pairs, num_shards, shard_id)

    output_dir = 'data/wikipedia/wikitext'
    output_file = os.path.join(output_dir, f'wikitext-{shard_id}.jsonl.bz2')
    os.makedirs(output_dir, exist_ok=True)

    logging.info(f'Processing {len(shard_pairs)} groups')
    with bz2.open(output_file, 'wb') as out:
        count = 0
        for start, end in shard_pairs:
            wikitexts = load_wikitext_from_multistream(_multistream_file, start, end)
            for title, page_id, wikitext in wikitexts:
                if title is None or len(title.strip()) == 0:
                    logging.warn('Entry missing a title')
                    continue
                if page_id is None:
                    logging.warn(f'Entry ({title}) is missing a page id')
                    continue
                if wikitext is None or len(wikitext.strip()) == 0:
                    logging.warn(f'Wikitext for ({title}, {page_id}) is `None` or empty')
                    continue

                data = {
                    'title': title,
                    'page_id': page_id,
                    'wikitext': wikitext
                }
                out.write(json.dumps(data).encode() + b'\n')

                count += 1
                if count % 1000 == 0:
                    logging.info(f'Processed {count} entries')

    logging.info('Terminating')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('shard_id', type=int)
    argp.add_argument('num_shards', type=int)
    args = argp.parse_args()
    main(args)
