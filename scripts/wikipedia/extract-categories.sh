#!/bin/sh
#$ -cwd

shard_id=$1

python -m wikicite.wikipedia.extract_categories ${shard_id}
