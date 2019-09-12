#!/bin/sh
#$ -cwd

shard_id=$1
num_shards=$2

python -m wikicite.wikipedia.extract_wikitext ${shard_id} ${num_shards}
