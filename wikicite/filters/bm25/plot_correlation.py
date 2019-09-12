import matplotlib
matplotlib.use('Agg')

import argparse
import json
import seaborn as sns
from scipy.stats.stats import pearsonr
from wikicite.filters.bm25.calculate_bm25 import calculate_bm25, load_df


def main(args):
    num_documents, avg_document_length, df = load_df(args.df_file)

    labels = []
    bm25s = []
    instances = []
    with open(args.label_file, 'r') as f:
        for line in f:
            data = json.loads(line)
            label = data['label']
            sentence = data['cloze'][0]
            document = data['document']
            bm25 = calculate_bm25(sentence, document, num_documents,
                                  avg_document_length, df)
            labels.append(label)
            bm25s.append(bm25)
            instances.append((data, label, bm25))

    print(pearsonr(labels, bm25s))
    ax = sns.scatterplot(bm25s, labels)
    ax.figure.savefig(args.plot_output_file)

    positives = list(filter(lambda t: t[1] == 1, instances))
    positives.sort(key=lambda t: t[2])


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('label_file')
    argp.add_argument('df_file')
    argp.add_argument('plot_output_file')
    args = argp.parse_args()
    main(args)
