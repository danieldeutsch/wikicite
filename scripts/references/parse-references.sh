#!/bin/sh
#$ -pe parallel-onenode 8
#$ -cwd

html_file_path=$1

python -m wikicite.references.parse_references ${html_file_path} \
  --num-cores 8
