import argparse
import gzip
import json
import numpy as np
import os
import spacy
from collections import defaultdict, Counter
from tqdm import tqdm
from typing import Dict, List


def load_df(file_path: str) -> defaultdict:
    df = defaultdict()
    num_documents = 0
    avg_document_length = 0.0
    with gzip.open(file_path, 'rb') as f:
        for i, line in tqdm(enumerate(f), desc=f'Loading DF scores'):
            line = line.decode().strip()
            if i == 0:
                num_documents = int(line)
            elif i == 1:
                avg_document_length = float(line)
            else:
                data = json.loads(line)
                df[data['word']] = data['df']
    return num_documents, avg_document_length, df


def calculate_bm25(nlp,
                   sentence: str,
                   document: List[str],
                   num_documents: int,
                   avg_document_length: float,
                   df: Dict[str, float],
                   k: float = 1.2,
                   b: float = 0.75) -> float:
    tf = Counter()
    for sent in document:
        for token in nlp(sent):
            tf[str(token).lower()] += 1
    document_length = sum(tf.values())

    bm25 = 0
    tokens = set(str(token).lower() for token in nlp(sentence))
    for token in tokens:
        token_df = df[token] if token in df else 0.0
        idf = np.log((num_documents + token_df + 0.5) / (token_df + 0.5))
        numerator = tf[token] * (k + 1)
        denominator = tf[token] + k * (1 - b + b * document_length / avg_document_length)
        bm25 += idf * numerator / denominator
    return bm25


def main(args):
    num_documents, avg_document_length, df = load_df(args.df_file)
    nlp = spacy.load('en')

    os.makedirs(os.path.dirname(args.output_file), exist_ok=True)
    with gzip.open(args.output_file, 'wb') as out:
        with gzip.open(args.input_file, 'rb') as f:
            for line in tqdm(f, desc=f'Processing {args.input_file}'):
                data = json.loads(line.decode())
                id_ = data['id']
                cloze = data['cloze']
                first_sentence = cloze[0]
                documents = data['documents']

                bm25s = []
                for document in documents:
                    bm25 = calculate_bm25(nlp, first_sentence, document, num_documents,
                                          avg_document_length, df)
                    bm25s.append(bm25)

                data = {
                    'id': id_,
                    'bm25s': bm25s
                }
                out.write(json.dumps(data).encode() + b'\n')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('input_file', help='The summary cloze file')
    argp.add_argument('df_file')
    argp.add_argument('output_file')
    args = argp.parse_args()
    main(args)
