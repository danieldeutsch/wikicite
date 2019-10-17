# WikiCite
WikiCite is a dataset of summary cloze instances gathered from Wikipedia.
Each paragraph on a Wikipedia page is viewed as a topic-focused summary of the reference documents cited within that paragraph.
For each sentence with a citation, we can use the previous sentences as the context, the cited document as the document to be summarized, and the cited sentence as the cloze.

This directory contains the code that was used to produce the WikiCite dataset.
The final dataset can be downloaded with the following links:
  - https://danieldeutsch.s3.amazonaws.com/summarize/data/wikicite/train.v1.1.jsonl.gz
  - https://danieldeutsch.s3.amazonaws.com/summarize/data/wikicite/valid.v1.1.jsonl.gz
  - https://danieldeutsch.s3.amazonaws.com/summarize/data/wikicite/test.v1.1.jsonl.gz

Tokenized versions can also be found here:
  - https://danieldeutsch.s3.amazonaws.com/summarize/data/wikicite/train.tokenized.v1.1.jsonl.gz
  - https://danieldeutsch.s3.amazonaws.com/summarize/data/wikicite/valid.tokenized.v1.1.jsonl.gz
  - https://danieldeutsch.s3.amazonaws.com/summarize/data/wikicite/test.tokenized.v1.1.jsonl.gz

See https://github.com/danieldeutsch/summarize for more information about the preprocessing code.

If you use this dataset, please use the following citation:
```
@inproceedings{DeutschRo19,
    author = {Daniel Deutsch and Dan Roth},
    title = {{Summary Cloze: A New Task for Content Selection in Topic-Focused Summarization}},
    booktitle = {Proc. of the Conference on Empirical Methods in Natural Language Processing (EMNLP)},
    year = {2019},
    url = "https://cogcomp.seas.upenn.edu/papers/DeutschRo19.pdf"
}
```

## Pipeline
First, the setup script will download the [Parsoid](https://github.com/wikimedia/parsoid) and [Unfluff](https://github.com/ageitgey/node-unfluff) libraries which are used to render wikitext into html and extract the body text from web documents, respectively.
```
sh scripts/setup.sh
```

### Wikipedia Preprocessing
Next, the Wikipedia articles need to be downloaded and preprocessed.
First, download the Wikipedia dump and corresponding xml file.
The dump version can be changed in this script.
```
sh scripts/wikipedia/download-dump.sh
```

The next steps involve extracting the wikitext from the xml, extracting the category information from the wikitext, rendering the wikitext into html documents (which are significantly easier to parse), and parsing the articles from the html.
This part of the pipeline splits the data into 1000 shards to make processing significantly faster using the NLP Grid.
```
sh scripts/wikipedia/extract-all-wikitext.sh
sh scripts/wikipedia/extract-all-categories.sh
sh scripts/wikipedia/render-all-html.sh
sh scripts/wikipedia/parse-all-articles.sh
```

### Common Crawl Index Setup
The reference documents will be scraped from [Common Crawl](http://commoncrawl.org/) data stored on AWS.
The crawls are too large to be downloaded, so we instead download the index files for the crawls, then look up the files where the urls which we need to scrape are located.
Not every url can be found in every monthly crawl, so we do this for several different crawls in order to increase the coverage of reference documents.
Each index requires downloading about 300gb of data.

We will assume that we are going to use the [`CC-MAIN-2019-09`](https://index.commoncrawl.org/CC-MAIN-2019-09) and [`CC-MAIN-2019-04`](https://index.commoncrawl.org/CC-MAIN-2019-04) crawls.
```
sh scripts/common-crawl/download-indexes.sh CC-MAIN-2019-09 CC-MAIN-2019-04
```
This script will download all the index files in serial to prevent overloading the AWS buckets.

### Reference Scraping
Now we need to obtain the body text for as many of the reference documents as we can find.
This is broken down into several steps: extracting the urls to scrape from the Wikipedia articles, finding the locations of the urls in Common Crawl using the local index files, splitting the locations into shards, extracting the html from the Common Crawl data stored on AWS, and finally parsing the body text from the html.
Scraping AWS runs in serial so the server isn't overwhelmed with traffic.
I found that 5 QPS was a decent speed, and scraping all of the urls took about a week.
```
sh scripts/references/extract-all-urls-to-crawl.sh
sh scripts/references/find-all-common-crawl-locations.sh CC-MAIN-2019-09 CC-MAIN-2019-04
sh scripts/references/split-common-crawl-locations.sh
sh scripts/references/scrape-all-common-crawl.sh
sh scripts/references/parse-all-references.sh
```
The script which scrapes Common Crawl has parameters which limit the QPS.
The scraping runs in serial to not overwhelm the server.
Generally, this part of the pipeline goes pretty fast, so use a low QPS to avoid overloading the AWS servers.

### Dataset Generation
This portion of the pipeline generates the full summary cloze dataset and then filters instances which are bad for some reason (non-English, likely a junk page, etc.).
To generate all of the training examples, run
```
python -m wikicite.cloze.generate_summary_cloze_data \
  data/summary-cloze/<date>/all.jsonl.gz
```

#### Non-English Filter
This filter removes any reference documents which are non-English.
The articles don't need to be filtered because they were parsed from English Wikipedia.

I had issues with using the python multiprocessing libraries (`concurrent.Futures`, `joblib`) because of an error with pickling some of the data.
I couldn't determine the cause of the error, so the script uses the `parallel` command instead:
```
qsub scripts/cloze/filter-non-english.sh data/summary-cloze/<date>
```

#### High-Quality Instance Classifier
Some of the data that is collected by the pipeline is low-quality: sometimes the reference document was a 404 response, the body text extraction is noisy, the cited url is incorrect, etc.
Therefore, we built a simple quality classifier that predicts whether or not an instance is "high-quality."
This section details how the training data was collected, how to compute the features for the classifier, and how to run the classifier.

##### Mechanical Turk
In order to build the classifier, we require supervision to decide if an instance is high-quality or low-quality.
To do so, we created a Mechanical Turk task that asks whether or not a reference document provides supporting evidence for the cloze.
Thus far, this data has only been annotated by me.

To sample instances for labeling, run
```
python -m wikicite.mturk.quality.random_sample \
  data/summary-cloze/<date>/english.jsonl.gz \
  sample.jsonl \
  <num-samples>

python -m wikicite.mturk.quality.preprocess sample.jsonl sample.csv
```
Then upload `sample.csv` to Mechanical Turk.
After the data has been annotated, you can run the post-processing script to reformat the Mechanical Turk output format
```
python -m wikicite.mturk.quality.postprocess sample-labeled.csv sample-labeled.jsonl
```
I have been keeping the judgment outputs from Mechanical Turk under `data/mturk/quality/judgments` and the full set of annotations in `data/mturk/quality/gold.jsonl`.
The `gold.jsonl` has more judgments than the `judgments` directory because I manually annotated some by hand before setting up the Mechanical Turk task.

##### Features
Some of the features used in the high-quality instance classifier require external resources.
The scripts to collect those resources are detailed below.

###### Document Frequencies
The BM25 feature computes a score between the cloze (the query) and the reference document.
This requires computing the document frequencies for every word.
These values can computed over the entire document corpus as follows:
```
python -m wikicite.filters.bm25.calculate_df \
  data/summary-cloze/<date>/english.jsonl.gz \
  data/filters/df.jsonl.gz \
  --num-cores <num-cores>
```

###### Page Views
This feature measures how popular each Wikipedia article is in terms of the total number of views of the page since 2015.
This script collects the view counts.
It runs rather slowly since it has to query several hundred thousand page views, so it should not be run too frequently.
There is a sleep command in the script to keep the requests per second at a reasonable level.
```
python -m wikicite.filters.get_page_views \
  data/summary-cloze/<date>/english.jsonl.gz \
  data/filters/views.jsonl.gz
```

##### Running the Classifier
The classifier can be trained via
```
sh scripts/cloze/train-quality-classifier.sh
```
This will train the classifier and output a precision-recall curve image and file with the corresponding threshold values to `pr.jsonl` and `pr.png`.
To filter the full dataset, run
```
qsub scripts/cloze/run-quality-classifier.sh data/summary-cloze/<date> <threshold>
```
For the release of the dataset, we chose a threshold of 1.1.
The classifier had high variance, but the output of the classifier seemed very reasonable.

### Generating the Final Dataset
This script will generate the final train/valid/test splits of the dataset, ensuring that the articles and reference documents are disjoint.
It also will rename the fields to more explicit names.
(This was done post-hoc because it would require changing the names in many places in the data generation code.)
```
qsub scripts/cloze/generate-final-splits.sh data/summary-cloze/<date>

python -m wikicite.cloze.verify data/summary-cloze/<date>/final
```

In version 1.1, a bug was corrected that did not correctly group together all of the reference documents cited within a sentence, only those which had the same offsets.
To ensure the dataset splits were the same as in 1.0, we added a postprocessing script to fix the problem.
```
python -m wikicite.cloze.postprocess <input-file-v1.0> <output-file-v1.1>
```

## Appendix
### Common Crawl Information
The index entries are stored based on their canonicalized urls.
[This file from pywb](https://github.com/webrecorder/pywb/blob/master/pywb/utils/canonicalize.py) contains the code to canonicalize a url.

The Common Crawl data is stored on AWS in the bucket `s3://commoncrawl`.
If you have the AWS command line tool installed, you can view the data with
```
aws s3 ls s3://commoncrawl
```
The indexes are stored under `s3://commoncrawl/cc-index/collections` or via http at `https://commoncrawl.s3.amazonaws.com/crawl-data/`.
