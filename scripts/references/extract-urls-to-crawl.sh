#!/bin/sh
#$ -cwd

shard_id=$1

python -m wikicite.references.extract_urls_to_crawl ${shard_id}
