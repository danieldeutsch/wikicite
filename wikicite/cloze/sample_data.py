import argparse
import bz2
import json
import os
import random


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)

    instances = bz2.open(args.input_file, 'r').read().decode().splitlines()
    random.shuffle(instances)

    for instance in instances:
        data = json.loads(instance)
        id_ = data['id']
        output_file = os.path.join(args.output_dir, f'{id_}.json')
        with open(output_file, 'w') as out:
            out.write(json.dumps(data, indent=2))



if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('input_file')
    argp.add_argument('output_dir')
    argp.add_argument('num_samples', type=int)
    args = argp.parse_args()
    main(args)
