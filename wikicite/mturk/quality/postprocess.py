import argparse
import csv
import json


def main(args):
    with open(args.output_jsonl, 'w') as out:
        with open(args.input_csv, 'r') as f:
            reader = csv.reader(f)
            for i, line in enumerate(reader):
                if i == 0:
                    header = line
                    json_column = header.index('Input.json')
                    label_column = header.index('Answer.category.label')
                else:
                    instance = json.loads(line[json_column])
                    label = line[label_column]
                    if label == 'Yes':
                        label = 1
                    elif label == 'No':
                        label = 0
                    else:
                        label = -1
                    instance['label'] = label
                    out.write(json.dumps(instance) + '\n')


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('input_csv')
    argp.add_argument('output_jsonl')
    args = argp.parse_args()
    main(args)
