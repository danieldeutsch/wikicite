import argparse
import gzip
import json
from tqdm import tqdm
from typing import Any, Dict, List, Set, Tuple


def load_instances(file_path: str) -> List[Dict[str, Any]]:
    instances = []
    with gzip.open(file_path, 'rb') as f:
        for line in tqdm(f, desc=f'Loading {file_path}'):
            instance = json.loads(line.decode())
            instances.append(instance)
    return instances


def get_pages_and_urls(instances: List[Dict[str, Any]]) -> Tuple[Set[int], Set[str]]:
    page_ids = set()
    urls = set()
    for instance in instances:
        page_id = instance['page_id']
        page_ids.add(page_id)
        for document in instance['documents']:
            url = document['canonical_url']
            urls.add(url)
    return page_ids, urls


def verify_fields(instances: List[Dict[str, Any]]) -> None:
    for instance in tqdm(instances, desc='Verifying'):
        assert instance['id']
        assert instance['page_title']
        assert instance['page_id'] is not None
        assert 'headings' in instance
        assert len(instance['left_context']) > 0
        for sentence in instance['left_context']:
            assert len(sentence) > 0
        assert len(instance['cloze']) > 0
        assert 'right_context' in instance
        for sentence in instance['right_context']:
            assert len(sentence) > 0
        assert len(instance['documents']) > 0
        for document in instance['documents']:
            assert document['canonical_url']
            assert len(document['paragraphs']) > 0
            for paragraph in document['paragraphs']:
                assert len(paragraph) > 0
                for sentence in paragraph:
                    assert sentence


def main(args):
    train = load_instances(f'{args.input_dir}/train.jsonl.gz')
    valid = load_instances(f'{args.input_dir}/valid.jsonl.gz')
    test = load_instances(f'{args.input_dir}/test.jsonl.gz')

    train_ids, train_urls = get_pages_and_urls(train)
    valid_ids, valid_urls = get_pages_and_urls(valid)
    test_ids, test_urls = get_pages_and_urls(test)

    assert len(train_ids & valid_ids) == 0
    assert len(train_ids & test_ids) == 0
    assert len(valid_ids & test_ids) == 0
    print('All pages are disjoint')

    assert len(train_urls & valid_urls) == 0
    assert len(train_urls & test_urls) == 0
    assert len(valid_urls & test_urls) == 0
    print('All documents are disjoint')

    verify_fields(train)
    verify_fields(valid)
    verify_fields(test)
    print('All fields validated')

    print()
    print(f'Found {len(train)} training instances')
    print(f'Found {len(valid)} validation instances')
    print(f'Found {len(test)} testing instances')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('input_dir')
    args = argp.parse_args()
    main(args)
