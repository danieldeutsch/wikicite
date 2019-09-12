#!/bin/sh
#$ -cwd
#$ -pe parallel-onenode 50
if [ "$#" -ne 2 ]; then
    echo "Usage: qsub scripts/cloze/run-quality-classifier.sh <cloze-dir> <threshold>"
    exit
fi

input_dir=$1
threshold=$2

python -m wikicite.filters.quality_classifier \
  predict \
  --input-jsonl ${input_dir}/english.jsonl.gz \
  --df-file data/filters/df.jsonl.gz \
  --views-file data/filters/views.jsonl.gz \
  --model-file data/filters/quality-model.pkl \
  --threshold ${threshold} \
  --good-jsonl ${input_dir}/high-quality.jsonl.gz \
  --bad-jsonl ${input_dir}/low-quality.jsonl.gz \
  --num-cores 50
