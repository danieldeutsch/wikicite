#!/bin/sh
#$ -cwd
#$ -l h=!(nlpgrid10|nlpgrid13|nlpgrid19)

shard_id=$1

python -m wikicite.wikipedia.render_html ${shard_id}
