#!/bin/sh
#$ -cwd
#$ -pe parallel-onenode 50
if [ "$#" -ne 1 ]; then
    echo "Usage: qsub scripts/cloze/filter-non-english.sh <cloze-dir>"
    exit
fi

input_dir=$1

zcat ${input_dir}/all.jsonl.gz | \
  parallel --pipe --jobs 50 --progress python -m wikicite.filters.filter_non_english | \
  gzip --stdout > ${input_dir}/english.jsonl.gz
