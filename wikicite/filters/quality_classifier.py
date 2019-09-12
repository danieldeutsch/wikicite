import matplotlib
matplotlib.use('Agg')

import warnings
warnings.filterwarnings('ignore', message='numpy.dtype size changed')

import argparse
import gzip
import json
import math
import matplotlib.pyplot as plt
import numpy as np
import random
import spacy
from collections import namedtuple
from joblib import Parallel, delayed
from sklearn.externals import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_recall_curve
from sklearn.utils.fixes import signature
from tqdm import tqdm
from typing import Any, Dict, Iterable, List

from wikicite.filters.bm25.calculate_bm25 import calculate_bm25, load_df


def load_views(file_path: str) -> Dict[int, int]:
    views = {}
    with gzip.open(file_path, 'rb') as f:
        for line in tqdm(f, desc='Loading views'):
            data = json.loads(line.decode())
            page_id = data['page_id']
            num_views = data['views']
            views[page_id] = num_views
    return views


def load_training_instances(file_path: str) -> List[Dict[str, Any]]:
    instances = []
    with open(file_path, 'r') as f:
        for line in f:
            data = json.loads(line)
            label = data['label']
            if label not in [0, 1]:
                continue
            instances.append(data)
    return instances


def load_prediction_instances(file_path: str) -> Iterable[Dict[str, Any]]:
    with gzip.open(file_path, 'rb') as f:
        for line in tqdm(f, desc=f'Loading instances from {file_path}'):
            data = json.loads(line.decode())
            yield data


def classify_documents(instances: List[Dict[str, Any]],
                       model: LogisticRegression,
                       num_documents: int,
                       avg_document_length: float,
                       df: Dict[str, int],
                       views: Dict[str, int],
                       threshold: float):
    nlp = spacy.load('en', disable=['parser', 'tagger', 'ner'])
    all_good_documents, all_bad_documents = [], []
    for instance in instances:
        page_id = instance['page_id']
        context = instance['context']
        sentence = instance['cloze'][0]

        good_documents = []
        bad_documents = []
        for document in instance['documents']:
            flat_document = [sentence for paragraph in document['paragraphs'] for sentence in paragraph]
            xs = extract_features(nlp,
                                  flat_document,
                                  sentence,
                                  context,
                                  page_id,
                                  df, num_documents, avg_document_length,
                                  views)

            xs = xs.reshape(1, -1)
            score = model.decision_function(xs)
            if score[0] >= threshold:
                good_documents.append(document)
            else:
                bad_documents.append(document)

        all_good_documents.append(good_documents)
        all_bad_documents.append(bad_documents)
    return instances, all_good_documents, all_bad_documents


def extract_features(nlp,
                     document: List[str],
                     sentence: str,
                     context: List[str],
                     page_id: int,
                     df: Dict[str, float],
                     num_documents: int,
                     avg_document_length: float,
                     views: Dict[int, int]) -> np.array:
    features = []

    # Flatten the document if necessary. This is because the data can be in
    # different formats because the paragraphs were added later
    if isinstance(document[0], list):
        # Paragraphs
        document = [sent for paragraph in document for sent in paragraph]

    # Number of document tokens
    features.append(np.log(sum(len(sent.split()) for sent in document)))

    # Number of sentence tokens
    features.append(np.log(len(sentence.split())))

    # BM25
    bm25 = calculate_bm25(nlp, sentence, document, num_documents, avg_document_length, df)
    features.append(np.log(bm25 + 1))

    # Views
    try:
        num_views = views[page_id]
    except KeyError:
        num_views = 1
    log_views = np.log(num_views)
    features.append(log_views)

    return np.array(features)


def extract_labels(instances: List[Dict[str, Any]]) -> np.array:
    labels = [instance['label'] for instance in instances]
    return np.array(labels)


def save_model(model: LogisticRegression, file_path: str) -> None:
    joblib.dump(model, file_path)


def load_model(file_path: str) -> LogisticRegression:
    return joblib.load(file_path)


def main(args):
    # Load BM25 dependencies
    num_documents, avg_document_length, df = load_df(args.df_file)
    # num_documents, avg_document_length, df = None, None, None

    # Load page views
    views = load_views(args.views_file)
    # views = None

    if args.mode == 'train':
        nlp = spacy.load('en', disable=['parser', 'tagger', 'ner'])

        # Load the data
        instances = load_training_instances(args.input_jsonl)
        random.shuffle(instances)

        # Run feature extraction
        print('Running feature extraction')
        X = []
        for instance in instances:
            xs = extract_features(nlp,
                                  instance['document'],
                                  instance['sentence'],
                                  instance['context'],
                                  instance['page_id'],
                                  df, num_documents, avg_document_length,
                                  views)
            X.append(xs)
        X = np.array(X)
        Y = extract_labels(instances)

        X_train, Y_train = X[args.num_validation:], Y[args.num_validation:]
        X_valid, Y_valid = X[:args.num_validation], Y[:args.num_validation]
        instances_train = instances[args.num_validation:]
        instances_valid = instances[:args.num_validation]

        # Train the model
        print(f'Training with {X_train.shape[0]} instances and {X_train.shape[1]} features')
        model = LogisticRegression()
        model.fit(X_train, Y_train)

        # Create the PR curve
        print(f'Validating on {X_valid.shape[0]} instances')
        Y_valid_scores = model.decision_function(X_valid)

        average_precision = average_precision_score(Y_valid, Y_valid_scores)
        precision, recall, thresholds = precision_recall_curve(Y_valid, Y_valid_scores)

        # In matplotlib < 1.5, plt.fill_between does not have a 'step' argument
        step_kwargs = ({'step': 'post'}
                       if 'step' in signature(plt.fill_between).parameters
                       else {})
        plt.step(recall, precision, color='b', alpha=0.2,
                 where='post')
        plt.fill_between(recall, precision, alpha=0.2, color='b', **step_kwargs)

        plt.xlabel('Recall')
        plt.ylabel('Precision')
        plt.ylim([0.0, 1.05])
        plt.xlim([0.0, 1.0])
        plt.title('2-class Precision-Recall curve: AP={0:0.2f}'.format(
                  average_precision))
        plt.savefig(args.pr_curve_file)

        with open(args.pr_threshold_file, 'w') as out:
            for p, r, threshold in zip(precision, recall, thresholds):
                out.write(json.dumps({
                    'precision': p,
                    'recall': r,
                    'f1': 2 * (p * r) / (p + r),
                    'threshold': threshold
                }) + '\n')

        print(f'Saving model to {args.model_file}')
        save_model(model, args.model_file)

        # Take some bad errors and output them to files
        scores_with_labels = list(zip(Y_valid_scores, Y_valid, instances_valid, X_valid))
        positive_labels = list(filter(lambda t: t[1] == 1, scores_with_labels))
        negative_labels = list(filter(lambda t: t[1] == 0, scores_with_labels))

        positive_labels.sort(key=lambda t: t[0])
        negative_labels.sort(key=lambda t: -t[0])

        # Take the top 10 "worst" errors from each
        with open(args.error_file, 'w') as out:
            for score, _, instance, features in positive_labels[:10] + negative_labels[:10]:
                instance['score'] = score
                instance['features'] = features.tolist()
                out.write(json.dumps(instance) + '\n')

    elif args.mode == 'predict':
        print(f'Loading model from {args.model_file}')
        model = load_model(args.model_file)

        instances = list(load_prediction_instances(args.input_jsonl))

        jobs = []
        batch_size = math.ceil(len(instances) / args.num_cores)
        for offset in range(0, len(instances), batch_size):
            batch = instances[offset:offset + batch_size]
            job = delayed(classify_documents)(batch, model, num_documents, avg_document_length, df, views, args.threshold)
            jobs.append(job)

        with gzip.open(args.good_jsonl, 'wb') as good_out:
            with gzip.open(args.bad_jsonl, 'wb') as bad_out:
                for instances, all_good_documents, all_bad_documents in Parallel(n_jobs=args.num_cores)(jobs):
                    for instance, good_documents, bad_documents in zip(instances, all_good_documents, all_bad_documents):
                        if len(good_documents) > 0:
                            del instance['documents']
                            instance['documents'] = good_documents
                            good_out.write(json.dumps(instance).encode() + b'\n')
                        if len(bad_documents) > 0:
                            del instance['documents']
                            instance['documents'] = bad_documents
                            bad_out.write(json.dumps(instance).encode() + b'\n')

    else:
        raise Exception(f'Unknown mode {args.mode}')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()

    subparsers = argp.add_subparsers(help='Which mode to run', dest='mode')
    train_parser = subparsers.add_parser('train')
    train_parser.add_argument('--input-jsonl', required=True, help='The input data for training and validation')
    train_parser.add_argument('--df-file', required=True, help='The document frequency file for BM25')
    train_parser.add_argument('--views-file', required=True, help='The views file')
    train_parser.add_argument('--model-file', required=True, help='The file to save the model')
    train_parser.add_argument('--pr-curve-file', required=True, help='The file to save the PR curve png')
    train_parser.add_argument('--pr-threshold-file', required=True, help='The file to save the PR curve threshold values')
    train_parser.add_argument('--num-validation', required=True, type=int, help='The number of validation examples to use')
    train_parser.add_argument('--error-file', required=True, help='The file to write some extreme errors to')

    predict_parser = subparsers.add_parser('predict')
    predict_parser.add_argument('--input-jsonl', required=True, help='The input data to assign scores to')
    predict_parser.add_argument('--df-file', required=True, help='The document frequency file for BM25')
    predict_parser.add_argument('--views-file', required=True, help='The views file')
    predict_parser.add_argument('--model-file', required=True, help='The file to save the model')
    predict_parser.add_argument('--threshold', required=True, type=float, help='The threshold to separate good from bad data')
    predict_parser.add_argument('--good-jsonl', required=True, help='The data above the threshold')
    predict_parser.add_argument('--bad-jsonl', required=True, help='The data below the threshold')
    predict_parser.add_argument('--num-cores', required=True, type=int, help='The number of cores to use for prediction')

    args = argp.parse_args()
    main(args)
