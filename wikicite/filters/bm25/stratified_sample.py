import argparse
import gzip
import json
import numpy as np
import os
import random
from tqdm import tqdm


def main(args):
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    bm25_file = 'data/filters/bm25/bm25.jsonl.gz'
    cloze_file = args.input_file

    # Load the BM25 scores
    bm25_scores = []
    id_to_bm25s = {}
    with gzip.open(bm25_file, 'rb') as f:
        for line in tqdm(f, desc='Loading bm25 scores'):
            data = json.loads(line.decode())
            instance_id = data['id']
            bm25_scores.extend(data['bm25s'])
            id_to_bm25s[instance_id] = data['bm25s']

    # Load the data
    id_to_instance = {}
    with gzip.open(cloze_file, 'rb') as f:
        for line in tqdm(f, desc='Loading data'):
            data = json.loads(line.decode())
            instance_id = data['id']
            id_to_instance[instance_id] = data

    # Compute the percentiles
    bm25_scores = np.array(bm25_scores)
    percentiles = np.percentile(bm25_scores, [10, 20, 30, 40, 50, 60, 70, 80, 90])
    min_value = np.min(bm25_scores)
    max_value = np.max(bm25_scores)

    # Split the data up into buckets
    buckets = [[] for _ in range(len(percentiles) + 1)]
    for instance_id, data in tqdm(id_to_instance.items()):
        instance_id = data['id']
        for i, score in enumerate(id_to_bm25s[instance_id]):
            bucket = np.searchsorted(percentiles, score)
            buckets[bucket].append((instance_id, i))

    # Sample from the buckets
    for i, bucket in enumerate(buckets):
        random.shuffle(bucket)
        sample = bucket[:args.num_samples_per_bucket]
        if i == 0:
            lower_bound = min_value
        else:
            lower_bound = percentiles[i - 1]
        if i == len(buckets) - 1:
            upper_bound = max_value
        else:
            upper_bound = percentiles[i]

        output_file = os.path.join(f'{output_dir}/{i}_{lower_bound:.2f}_{upper_bound:.2f}.jsonl')
        with open(output_file, 'w') as out:
            for instance_id, index in sample:
                data = id_to_instance[instance_id]
                output_data = {
                    'id': instance_id,
                    'context': data['context'],
                    'cloze': data['cloze'],
                    'document': data['documents'][index]
                }
                out.write(json.dumps(output_data) + '\n')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('input_file')
    argp.add_argument('output_dir')
    argp.add_argument('--num-samples-per-bucket', type=int, default=10)
    args = argp.parse_args()
    main(args)
