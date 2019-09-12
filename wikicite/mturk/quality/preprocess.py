import argparse
import csv
import json
import spacy
import sys
from collections import Counter
from tqdm import tqdm
from typing import Dict, List, Set, Tuple

nlp = spacy.load('en')


def _get_top_entities(sentence_doc: spacy.tokens.doc.Doc, document_docs: List[List[spacy.tokens.doc.Doc]]) -> Dict[str, str]:
    # Gets the most frequent entities which appear in both the document and the sentence
    sentence_counts = Counter(map(lambda e: str(e), sentence_doc.ents))
    document_docs = [doc for paragraph in document_docs for doc in paragraph]
    document_counts = Counter(str(e) for doc in document_docs for e in map(lambda e: str(e), doc.ents))
    intersection = sentence_counts & document_counts
    top_k = set([entity for entity, _ in intersection.most_common(3)])
    colors = ['#2978A0', '#631A86', '#F26419']
    color_map = {}
    for i, entity in enumerate(top_k):
        color_map[entity] = colors[i]
    return color_map


def _insert_highlights(doc: spacy.tokens.doc.Doc, color_map: Dict[str, str]) -> str:
    string = str(doc)
    entities = list(doc.ents)
    entities = list(filter(lambda e: str(e) in color_map, entities))
    entities.sort(key=lambda e: -e.start_char)
    for entity in entities:
        color = color_map[str(entity)]
        start, end = entity.start_char, entity.end_char
        prefix, infix, suffix = string[:start], string[start:end], string[end:]
        string = f'{prefix}<font color="{color}">{infix}</font>{suffix}'
    return string


def preprocess(context: List[str], sentence: str, document: List[List[str]]) -> Tuple[str, str, str]:
    if False:
        # Run NER on all of the text to count the number of entities which appear
        context_docs = [nlp(x) for x in context]
        sentence_doc = nlp(sentence)
        document_docs = [[nlp(sent) for sent in paragraph] for paragraph in document]

        color_map = _get_top_entities(sentence_doc, document_docs)

        # Insert highlights
        context = ' '.join([_insert_highlights(doc, color_map) for doc in context_docs])
        sentence = _insert_highlights(sentence_doc, color_map)

        document = []
        for paragraph_docs in document_docs:
            paragraph = '<p>' + ' '.join([_insert_highlights(doc, color_map) for doc in paragraph_docs]) + '</p>'
            document.append(paragraph)
        document = ' '.join(document)
    else:
        context = ' '.join(context)
        document = ' '.join(['<p>' + ' '.join([sentence for sentence in paragraph]) + '</p>' for paragraph in document])

    # Carriage returns mess up mechanical turk's csv parser
    context = context.replace('\r', '\n')
    sentence = sentence.replace('\r', '\n')
    document = document.replace('\r', '\n')

    return context, sentence, document


def main(args):
    with open(args.input_jsonl, 'r') as f:
        with open(args.output_csv, 'w') as out:
            writer = csv.writer(out)
            writer.writerow(['json', 'context', 'sentence', 'document'])
            for line in tqdm(f):
                line = line.strip()
                data = json.loads(line)
                context, sentence, document = preprocess(data['context'], data['sentence'], data['document'])
                writer.writerow([line, context, sentence, document])


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('input_jsonl')
    argp.add_argument('output_csv')
    args = argp.parse_args()
    main(args)
