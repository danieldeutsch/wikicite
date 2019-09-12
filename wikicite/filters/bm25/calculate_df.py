import argparse
import gzip
import json
import math
import os
import spacy
from collections import Counter
from joblib import Parallel, delayed
from tqdm import tqdm
from typing import List


def calculate_df(documents: List[List[str]]):
    nlp = spacy.load('en', disable=['parser', 'tagger', 'ner'])
    df = Counter()
    num_documents = 0
    total_document_length = 0
    for document in documents:
        words = set()
        for sentence in document:
            for token in nlp(sentence):
                word = str(token).lower().strip()
                if word:
                    words.add(word)
                    total_document_length += 1

        for word in words:
            df[word] += 1
        num_documents += 1
    return df, num_documents, total_document_length


def load_documents(file_path: str) -> List[List[str]]:
    all_documents = []
    with gzip.open(args.input_file, 'rb') as f:
        for line in tqdm(f, desc=f'Loading {file_path}'):
            data = json.loads(line.decode())
            documents = data['documents']
            for document in documents:
                flat_document = [sentence for paragraph in document['paragraphs'] for sentence in paragraph]
                all_documents.append(flat_document)
    return all_documents


def main(args):
    documents = load_documents(args.input_file)

    # Create all of the jobs
    num_cores = args.num_cores
    batch_size = math.ceil(len(documents) / num_cores)
    jobs = []
    for offset in range(0, len(documents), batch_size):
        batch = documents[offset:offset + batch_size]
        jobs.append(delayed(calculate_df)(batch))

    # Aggregate the results
    df = Counter()
    num_documents = 0
    total_document_length = 0
    for batch_df, batch_num_documents, batch_total_document_length in Parallel(n_jobs=num_cores)(jobs):
        df += batch_df
        num_documents += batch_num_documents
        total_document_length += batch_total_document_length
    avg_document_length = total_document_length / num_documents

    # Save the results
    dirname = os.path.dirname(args.output_file)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    with gzip.open(args.output_file, 'w') as out:
        out.write(f'{num_documents}\n'.encode())
        out.write(f'{avg_document_length}\n'.encode())
        for word in tqdm(sorted(df.keys()), desc=f'Writing results to {args.output_file}'):
            count = df[word]
            data = {
                'word': word,
                'df': count
            }
            out.write(json.dumps(data).encode() + b'\n')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('input_file', help='The summary cloze file')
    argp.add_argument('output_file')
    argp.add_argument('--num-cores', type=int, default=16)
    args = argp.parse_args()
    main(args)
