#!/bin/sh
#$ -cwd

index_name=$1
indx_file_path=$2

python -m wikicite.references.find_common_crawl_locations ${index_name} ${indx_file_path}
