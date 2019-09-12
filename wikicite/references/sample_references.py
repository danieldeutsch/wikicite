import argparse
import bz2
import json
import os
import random
import uuid
from tqdm import tqdm
from typing import Any, Dict, List

from wikicite.references.scrape_references import get_url_to_scrape, is_scrapable


def load_reference_htmls(file_path: str) -> Dict[str, Dict[str, Any]]:
    htmls = {}
    with bz2.open(file_path, 'rb') as f:
        for line in tqdm(f, desc=f'Loading htmls from {file_path}'):
            data = json.loads(line.decode())
            htmls[data['url']] = data
    return htmls


def load_documents(file_path: str) -> Dict[str, Dict[str, Any]]:
    documents = {}
    with bz2.open(file_path, 'rb') as f:
        for line in tqdm(f, desc=f'Loading documents from {file_path}'):
            data = json.loads(line.decode())
            documents[data['url']] = data
    return documents


def load_reference_metadata(file_path: str) -> List[Dict[str, str]]:
    metadatas = []
    with bz2.open(file_path, 'rb') as f:
        for line in tqdm(f, desc=f'Loading metadata from {file_path}'):
            data = json.loads(line.decode())
            references = data['references']
            for reference in references.values():
                metadatas.append(reference)
    return metadatas



def main(args):
    html_file_path = f'data/references/html/html-0.jsonl.bz2'
    documents_file_path = f'data/references/documents/documents-0.jsonl.bz2'
    article_file_path = f'data/wikipedia/articles/articles-0.jsonl.bz2'

    htmls = load_reference_htmls(html_file_path)
    documents = load_documents(documents_file_path)
    metadatas = load_reference_metadata(article_file_path)

    total_metadata = len(metadatas)
    metadata_with_urls = 0
    scrapable_urls = 0
    has_html = 0
    has_body_text = 0

    all_output_data = []
    data_with_html = []
    data_with_text = []

    for metadata in tqdm(metadatas, desc='Processing metadata'):
        id_ = str(uuid.uuid4())
        output_data = {}
        output_data['id'] = id_
        output_data['metadata'] = metadata

        url = get_url_to_scrape(metadata)
        html, sentences = None, None
        if url:
            metadata_with_urls += 1
            if is_scrapable(url, True):
                scrapable_urls += 1

            output_data['url'] = url
            if url in htmls:
                html = htmls[url].pop('html', None)
                output_data['scrape'] = htmls[url]
                if html:
                    has_html += 1
            if url in documents:
                sentences = documents[url]['text']
                if sentences:
                    has_body_text += 1

        all_output_data.append((id_, output_data, html, sentences))
        if html:
            data_with_html.append((id_, output_data, html, sentences))
        if sentences:
            data_with_text.append((id_, output_data, html, sentences))


    all_output_data = data_with_text

    random.shuffle(all_output_data)

    print('Total', total_metadata)
    print('Total with a url', metadata_with_urls)
    print('Total with a scrapable url', scrapable_urls)
    print('Total with html', has_html)
    print('Total with body text', has_body_text)

    for id_, output_data, html, sentences in tqdm(all_output_data[:args.sample_size],
                                                  desc='Saving samples'):
        output_dir = os.path.join(args.output_dir, id_)
        os.makedirs(output_dir, exist_ok=True)

        data_file = os.path.join(output_dir, 'data.json')
        html_file = os.path.join(output_dir, 'reference.html')
        sentences_file = os.path.join(output_dir, 'sentences.json')

        with open(data_file, 'w') as out:
            out.write(json.dumps(output_data, indent=2))
        if html:
            with open(html_file, 'w') as out:
                out.write(html)
        if sentences:
            with open(sentences_file, 'w') as out:
                out.write(json.dumps(sentences, indent=2))


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('shard_id', type=int)
    argp.add_argument('sample_size', type=int)
    argp.add_argument('output_dir')
    args = argp.parse_args()
    main(args)
