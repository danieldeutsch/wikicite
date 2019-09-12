import argparse
import gzip
import json
import numpy as np
from collections import Counter, defaultdict
from tqdm import tqdm
from typing import Any, Dict, List


def get_metrics(instances: List[Dict[str, Any]]) -> Dict[str, float]:
    num_instances = len(instances)
    num_singledoc, num_multidoc = 0, 0
    num_document_tokens_list = []
    num_document_sentences_list = []
    num_context_tokens_list = []
    num_context_sentences_list = []
    num_cloze_tokens_list = []
    num_topic_tokens_list = []
    document_counts = Counter()
    document_to_pages = defaultdict(set)

    for instance in tqdm(instances, desc='Computing statistics'):
        documents = instance['documents']
        context = instance['left_context']
        cloze = instance['cloze']
        topic = ' '.join([instance['page_title']] + instance['headings'])

        if len(documents) == 1:
            num_singledoc += 1
        else:
            num_multidoc += 1

        for document in documents:
            num_document_tokens_list.append(sum(len(sentence.split()) for paragraph in document['paragraphs'] for sentence in paragraph))
            num_document_sentences_list.append(sum(len(paragraph) for paragraph in document['paragraphs']))
            if document['canonical_url'] is not None:
                url = document['canonical_url']
            else:
                url = document['url']
            document_counts[url] += 1
            document_to_pages[url].add(instance['page_id'])

        num_context_tokens_list.append(sum(len(sentence.split()) for sentence in context))
        num_context_sentences_list.append(len(context))
        num_cloze_tokens_list.append(len(cloze.split()))
        num_topic_tokens_list.append(len(topic.split()))

    num_unique_documents = len(document_counts)
    total_citations = sum(document_counts.values())
    num_documents_used_multiple_times = sum(count > 1 for count in document_counts.values())
    num_documents_used_multiple_pages = sum(len(pages) > 1 for pages in document_to_pages.values())

    return {
        'num_instances': num_instances,
        'num_singledoc': num_singledoc,
        'num_multidoc': num_multidoc,
        'num_document_tokens': f'{np.average(num_document_tokens_list):.2f} ({np.std(num_document_tokens_list):.2f})',
        'num_document_sentences': f'{np.average(num_document_sentences_list):.2f} ({np.std(num_document_sentences_list):.2f})',
        'num_context_tokens': f'{np.average(num_context_tokens_list):.2f} ({np.std(num_context_tokens_list):.2f})',
        'num_context_sentences': f'{np.average(num_context_sentences_list):.2f} ({np.std(num_context_sentences_list):.2f})',
        'num_cloze_tokens': f'{np.average(num_cloze_tokens_list):.2f} ({np.std(num_cloze_tokens_list):.2f})',
        'num_topic_tokens': f'{np.average(num_topic_tokens_list):.2f} ({np.std(num_topic_tokens_list):.2f})',
        'num_unique_documents': num_unique_documents,
        'total_citations': total_citations,
        'num_documents_used_multiple_times': num_documents_used_multiple_times,
        'num_documents_used_multiple_pages': num_documents_used_multiple_pages
    }


def load_instances(file_path: str) -> List[Dict[str, Any]]:
    instances = []
    with gzip.open(file_path, 'rb') as f:
        for line in tqdm(f, desc=f'Loading {file_path}'):
            instance = json.loads(line)
            instances.append(instance)
    return instances


def main(args):
    train = load_instances(args.train_jsonl)
    valid = load_instances(args.valid_jsonl)
    test = load_instances(args.test_jsonl)

    metrics = {
        'all': get_metrics(train + valid + test),
        'train': get_metrics(train),
        'valid': get_metrics(valid),
        'test': get_metrics(test)
    }
    print(json.dumps(metrics, indent=4))


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('train_jsonl')
    argp.add_argument('valid_jsonl')
    argp.add_argument('test_jsonl')
    args = argp.parse_args()
    main(args)
