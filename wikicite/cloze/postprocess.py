import argparse
import gzip
import json
from collections import defaultdict
from tqdm import tqdm


def main(args):
    page_id_to_instances = defaultdict(list)
    with gzip.open(args.input_jsonl, 'rb') as f:
        for line in tqdm(f):
            data = json.loads(line.decode())
            page_id = data['page_id']
            page_id_to_instances[page_id].append(data)

    with gzip.open(args.output_jsonl, 'wb') as out:
        for page_id, instances in page_id_to_instances.items():
            context_to_instances = defaultdict(list)
            for instance in instances:
                left = ' '.join(instance['left_context'])
                cloze = instance['cloze']
                right = ' '.join(instance['right_context'])
                context_to_instances[(left, cloze, right)].append(instance)

            for context_instances in context_to_instances.values():
                seen_documents = set()
                documents = []
                for instance in context_instances:
                    for document in instance['documents']:
                        url = document['canonical_url']
                        if url not in seen_documents:
                            documents.append(document)
                            seen_documents.add(url)
                
                instance = context_instances[0]
                instance['documents'] = documents
                out.write(json.dumps(instance).encode() + '\n'.encode())


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('input_jsonl')
    argp.add_argument('output_jsonl')
    args = argp.parse_args()
    main(args)

