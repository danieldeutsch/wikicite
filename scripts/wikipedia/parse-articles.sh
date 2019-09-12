#!/bin/sh
#$ -cwd
#$ -l h=!(nlpgrid10|nlpgrid11|nlpgrid19)

shard_id=$1

OMP_NUM_THREADS=1 python -m wikicite.wikipedia.parse_articles ${shard_id}
