#!/bin/sh
#$ -cwd
#$ -pe parallel-onenode 1
set -e
if [ "$#" -ne 0 ]; then
    echo "Usage: qsub scripts/references/scrape-all-common-crawl.sh"
    exit
fi

# Scrape the data in serial to not overwhelm the server
for locations_file_path in $(find data/references/common-crawl-locations/*.bz2); do
  python -m wikicite.references.scrape_common_crawl ${locations_file_path}
  if [[ $? == 0 ]]; then
    :
  else
    echo "Exiting early";
    exit
  fi
done
