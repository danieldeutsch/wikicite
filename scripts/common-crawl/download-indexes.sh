#!/bin/sh
#$ -cwd
if [ "$#" -eq 0 ]; then
    echo "Usage: qsub scripts/common-crawl/download-indexes.sh <index-name>+"
    echo "  e.g. qsub scripts/common-crawl/download-indexes.sh CC-MAIN-2019-09 CC-MAIN-2019-04"
    exit
fi

for index_name in "$@"; do
  index_dir="data/common-crawl/collections/${index_name}"
  mkdir -p ${index_dir}

  # Download the list of index files
  index_paths_file="${index_dir}/cc-index.paths.gz"
  if [ ! -f ${index_paths_file} ]; then
    wget https://commoncrawl.s3.amazonaws.com/crawl-data/${index_name}/cc-index.paths.gz -O ${index_paths_file}
  fi

  for file_path in $(zcat ${index_paths_file} | grep .gz); do
    filename=$(basename ${file_path})
    target_path="${index_dir}/${filename}"
    if [ ! -f ${target_path} ]; then
      url="https://commoncrawl.s3.amazonaws.com/${file_path}"
      wget -q ${url} -O ${target_path}
    fi
  done
done
