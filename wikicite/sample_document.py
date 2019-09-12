import argparse
import bz2
import json
import os
from typing import Any, Dict, List


def load_documents(file_path: str) -> List[Dict[str, Any]]:
    documents = []
    with bz2.open(file_path, 'rb') as f:
        for line in f:
            document = json.loads(line.decode())
            documents.append(document)
    return documents


def load_references(file_path: str) -> Dict[str, str]:
    references = {}
    with bz2.open(file_path, 'rb') as f:
        for line in f:
            reference = json.loads(line.decode())
            url = reference['url']
            references[url] = reference
    return references


def get_headings_html(headings: List[str]) -> str:
    headings = list(filter(None, headings))
    if not headings:
        return None
    lowest_index = len(headings)
    lowest_heading = headings[-1]
    return f'<h{lowest_index}>{lowest_heading}</h{lowest_index}>'


def get_paragraph_html(paragraph: Dict[str, Any],
                       document_citations: Dict[int, Any],
                       references: Dict[str, Any]) -> str:
    # Sort the citations in reverse order
    citations = paragraph['citations']
    citations.sort(key=lambda c: c['offset'], reverse=True)

    citations_to_save = set()

    text = paragraph['text']
    for citation in citations:
        id_ = str(citation['reference_id'])
        offset = citation['offset']

        citation_html = f'[{id_}]'
        if id_ in document_citations and 'url' in document_citations[id_]:
            url = document_citations[id_]['url']
            if url in references:
                citations_to_save.add(id_)
                citation_html = f'<a href="{id_}.json">[{id_}]</a>'
        text = text[:offset] + citation_html + text[offset:]

    return f'<p>{text}</p>', citations_to_save


def main(args):
    params = json.load(open(args.config, 'r'))
    data_dir = params['data_dir']

    shard_id = args.shard_id
    output_dir = args.output_dir

    os.makedirs(output_dir, exist_ok=True)

    document_file = os.path.join(data_dir, 'documents', f'documents-{shard_id}.jsonl.bz2')
    reference_file = os.path.join(data_dir, 'reference-text', f'reference-text-{shard_id}.jsonl.bz2')

    documents = load_documents(document_file)
    references = load_references(reference_file)

    for document in documents:
        # Check to see if at least 1 reference for this document exists.
        # If it doesn't then we should not continue
        ok = False
        for reference in document['references'].values():
            if 'url' in reference and reference['url'] in references:
                ok = True
                break
        if not ok:
            continue

        title = document['title']
        document_references = document['references']
        document_dir = os.path.join(output_dir, title)
        os.makedirs(document_dir, exist_ok=True)

        # Save the article
        article_file = os.path.join(document_dir, 'article.html')
        citations_to_save = set()

        print(f'Writing article {title} to {article_file}')
        with open(article_file, 'w') as out:
            for section in document['sections']:
                headings = get_headings_html(section['headings'])
                if headings:
                    out.write(headings + '\n')

                for paragraph in section['paragraphs']:
                    text, paragraph_citations = get_paragraph_html(paragraph, document_references, references)
                    citations_to_save |= paragraph_citations
                    out.write(text + '\n')

        # Save all of the citations
        for id_ in citations_to_save:
            citation = document_references[id_]
            url = citation['url']
            reference = references[url]
            citation_file = os.path.join(document_dir, f'{id_}.json')
            print(f'Writing citation {id_} to {citation_file}')
            with open(citation_file, 'w') as out:
                out.write(json.dumps(reference, indent=2))


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('config')
    argp.add_argument('shard_id', type=int)
    argp.add_argument('output_dir')
    args = argp.parse_args()
    main(args)
