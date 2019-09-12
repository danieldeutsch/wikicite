import argparse
import gzip
import json
import os
import random
from tqdm import tqdm
from uuid import uuid4


def main(args):
    lines = []
    with gzip.open(args.input_file, 'rb') as f:
        for line in tqdm(f):
            line = line.decode()
            lines.append(line)

    random.shuffle(lines)
    dir_name = os.path.dirname(args.output_file)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
    with open(args.output_file, 'w') as out:
        count = 0
        done = False
        for line in lines:
            data = json.loads(line)
            for i, document in enumerate(data['documents']):
                instance = {
                    'id': str(uuid4()),
                    'instance_id': data['id'],
                    'page_title': data['page_title'],
                    'page_id': data['page_id'],
                    'headings': data['headings'],
                    'document_index': i,
                    'document': document['paragraphs'],
                    'context': data['context'],
                    'cloze': data['cloze'],
                    'sentence': data['cloze'][0]
                }
                out.write(json.dumps(instance) + '\n')
                count += 1
                if count >= args.num_samples:
                    done = True
                    break

            if done:
                break


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('input_file')
    argp.add_argument('output_file')
    argp.add_argument('num_samples', type=int)
    args = argp.parse_args()
    main(args)
