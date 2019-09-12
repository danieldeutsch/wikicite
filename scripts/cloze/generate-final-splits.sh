#!/bin/sh
#$ -cwd
#$ -pe parallel-onenode 50
if [ "$#" -ne 1 ]; then
    echo "Usage: qsub scripts/cloze/generate-final-splits.sh <cloze-dir>"
    exit
fi

input_dir=$1

python -m wikicite.cloze.generate_final_data_splits \
  ${input_dir}/high-quality.jsonl.gz \
  ${input_dir}/final \
  10000 \
  10000
