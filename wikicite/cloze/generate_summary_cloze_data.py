import argparse
import bz2
import gzip
import json
import os
import re
from dateutil import parser
from glob import glob
from pywb.utils.canonicalize import UrlCanonicalizeException, canonicalize
from tqdm import tqdm
from typing import Any, Dict, List, Set
from uuid import uuid4

from wikicite.references.extract_urls_to_crawl import get_url_to_scrape


def load_jsonl(file_path: str) -> List[Dict[str, Any]]:
    items = []
    with bz2.open(file_path, 'rb') as f:
        for line in f:
            data = json.loads(line.decode())
            items.append(data)
    return items


def load_all_documents(document_file_glob: str) -> Dict[str, Dict[str, Any]]:
    documents = {}
    for document_file_path in tqdm(glob(document_file_glob), desc='Loading documents'):
        if re.match(r'.*documents-\d+.jsonl.bz2', document_file_path):
            with bz2.open(document_file_path, 'rb') as f:
                for line in f:
                    data = json.loads(line.decode())
                    canonical_url = data['canonical_url']
                    documents[canonical_url] = data
    return documents


def load_living_people(category_dir: str) -> Set[int]:
    living_people = set()
    for category_file in tqdm(glob(f'{category_dir}/*.bz2'), desc='Loading living people'):
        with bz2.open(category_file, 'rb') as f:
            for line in f:
                data = json.loads(line.decode())
                if 'Living people' in data['categories']:
                    page_id = data['page_id']
                    living_people.add(page_id)
    return living_people


def map_offset_to_reference_ids(citations: List[Dict[str, int]]) -> Dict[int, List[int]]:
    offset_to_references = {}
    for citation in citations:
        offset = citation['offset']
        reference_id = citation['reference_id']
        if offset in offset_to_references:
            offset_to_references[offset].append(reference_id)
        else:
            offset_to_references[offset] = [reference_id]
    return offset_to_references


def main(args):
    output_file = args.output_file
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)

    if not output_file.endswith('.gz'):
        raise Exception(f'The output file must end in ".gz"')

    living_people = load_living_people('data/wikipedia/categories')

    document_file_glob = 'data/references/documents/documents-*.jsonl.bz2'
    documents = load_all_documents(document_file_glob)

    article_dir = 'data/wikipedia/articles'

    with gzip.open(output_file, 'wb') as out:
        for article_file_path in tqdm(sorted(glob(f'{article_dir}/articles-*.jsonl.bz2')), desc='Processing articles'):
            articles = load_jsonl(article_file_path)
            for article in tqdm(articles):
                page_title = article['title']
                page_id = article['page_id']
                if page_id not in living_people:
                    continue
                reference_metadata = article['references']
                reference_metadata = {int(id_): reference for id_, reference in reference_metadata.items()}

                for section in article['sections']:
                    for paragraph in section['paragraphs']:
                        sentence_offsets = paragraph['sentence_offsets']
                        citations = paragraph['citations']
                        offset_to_reference_id = map_offset_to_reference_ids(citations)
                        text = paragraph['text']
                        sentences = [text[start:end].strip() for start, end in sentence_offsets]

                        # Skip the first sentence because we require a non-empty context
                        for i, (sentence, (start, end)) in enumerate(zip(sentences[1:], sentence_offsets[1:])):
                            sentence_index = i + 1
                            for offset, reference_ids in offset_to_reference_id.items():
                                reference_documents = []
                                if start <= offset and offset <= end:
                                    # The citation is in this sentence
                                    for reference_id in reference_ids:
                                        if reference_id in reference_metadata:
                                            metadata = reference_metadata[reference_id]
                                            url = get_url_to_scrape(metadata)
                                            if not url:
                                                continue
                                            try:
                                                canonical_url = canonicalize(url)
                                                if canonical_url in documents:
                                                    document = documents[canonical_url]
                                                    if len(document['paragraphs']) > 0:
                                                        url = document['canonicalLink'] if 'canonicalLink' in document else url
                                                        date = document['date'] if 'date' in document else None
                                                        if date is not None:
                                                            try:
                                                                date = str(parser.parse(date))
                                                            except:
                                                                pass
                                                        title = document['title'] if 'title' in document else None
                                                        # The offset into the sentence where this citation is
                                                        sentence_offset = offset - start
                                                        document_data = {
                                                            'url': url,
                                                            'canonical_url': canonical_url,
                                                            'date': date,
                                                            'title': title,
                                                            'offset': sentence_offset,
                                                            'paragraphs': document['paragraphs']
                                                        }
                                                        reference_documents.append(document_data)
                                            except UrlCanonicalizeException:
                                                continue

                                if reference_documents:
                                    context = sentences[:sentence_index]
                                    cloze = sentences[sentence_index:]
                                    headings = list(filter(None, section['headings']))
                                    output_data = {
                                        'id': str(uuid4()),
                                        'page_title': page_title,
                                        'page_id': page_id,
                                        'headings': headings,
                                        'documents': reference_documents,
                                        'context': context,
                                        'cloze': cloze
                                    }
                                    out.write(json.dumps(output_data).encode() + b'\n')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('output_file')
    args = argp.parse_args()
    main(args)
