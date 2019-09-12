import argparse
import gzip
import json
import networkx as nx
import os
import random
from collections import defaultdict
from tqdm import tqdm
from typing import Dict, List, T, Tuple


def take_until_size(components: List[nx.Graph],
                    index: int,
                    node_id_to_instances: Dict[str, List[T]],
                    target_size: int) -> Tuple[List[T], int]:
    split = []
    for i in range(index, len(components)):
        component = components[i]
        for node_id in component:
            if node_id in node_id_to_instances:
                # This is a page
                split.extend(node_id_to_instances[node_id])
            else:
                # This is a reference document
                pass
        if target_size is not None and len(split) >= target_size:
            return split, i + 1
    return split, len(components) + 1


def save_instances(instances: List[T], file_path: str) -> None:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with gzip.open(file_path, 'wb') as out:
        for instance in tqdm(instances, desc=f'Saving to {file_path}'):
            # Rename the fields to be more clear about what they are exactly and
            # make it easier for dataset readers to read in the input
            context = instance['context']
            cloze = instance['cloze']
            del instance['context']
            del instance['cloze']
            sentence = cloze[0]
            cloze = cloze[1:]

            instance['left_context'] = context
            instance['cloze'] = sentence
            instance['right_context'] = cloze

            out.write(json.dumps(instance).encode() + b'\n')


def main(args):
    # We will ensure that no page or reference document is present in two
    # of train/valid/test. To do so, we will create a bipartite graph where
    # one set of nodes is the pages and the other is the reference documents.
    # Then we will take connected components until the desired split sizes are met
    graph = nx.Graph()
    page_id_to_node_id = {}
    url_to_node_id = {}
    node_id_to_instances = defaultdict(list)

    # Create the graph
    with gzip.open(args.input_jsonl, 'rb') as f:
        for line in tqdm(f):
            data = json.loads(line.decode())

            # Add the node for the page
            page_id = data['page_id']
            if page_id in page_id_to_node_id:
                page_node_id = page_id_to_node_id[page_id]
            else:
                page_node_id = len(graph)
                page_id_to_node_id[page_id] = page_node_id
                graph.add_node(page_node_id)

            node_id_to_instances[page_node_id].append(data)

            # Add the nodes for the documents
            documents = data['documents']
            for document in documents:
                url = document['canonical_url']
                if url in url_to_node_id:
                    url_node_id = url_to_node_id[url]
                else:
                    url_node_id = len(graph)
                    url_to_node_id[url] = url_node_id
                    graph.add_node(url_node_id)

                # Add the connecting edge
                graph.add_edge(page_node_id, url_node_id)

    # Get the connected components and split them into sets
    components = list(nx.connected_components(graph))
    random.shuffle(components)

    valid, index = take_until_size(components, 0, node_id_to_instances, args.valid_size)
    test, index = take_until_size(components, index, node_id_to_instances, args.test_size)
    train, _ = take_until_size(components, index, node_id_to_instances, None)

    train_file = os.path.join(args.output_dir, 'train.jsonl.gz')
    valid_file = os.path.join(args.output_dir, 'valid.jsonl.gz')
    test_file = os.path.join(args.output_dir, 'test.jsonl.gz')
    save_instances(train, train_file)
    save_instances(valid, valid_file)
    save_instances(test, test_file)


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('input_jsonl')
    argp.add_argument('output_dir')
    argp.add_argument('valid_size', type=int)
    argp.add_argument('test_size', type=int)
    args = argp.parse_args()
    main(args)
