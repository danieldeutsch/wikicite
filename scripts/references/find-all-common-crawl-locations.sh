
if [ "$#" -eq 0 ]; then
    echo "Usage: sh scripts/references/find-all-common-crawl-locations.sh <index-name>+"
    exit
fi

for index_name in "$@"; do
  log_dir="logs/references/common-crawl-locations/${index_name}"

  mkdir -p ${log_dir}
  for index_file_path in $(find data/common-crawl/collections/${index_name}/cdx-*.gz); do
    filename=$(basename ${index_file_path})
    qsub -N "find-index-locations-${filename}" -o "${log_dir}/${filename}.stdout" -e "${log_dir}/${filename}.stderr" \
      scripts/references/find-common-crawl-locations.sh ${index_name} ${index_file_path}
  done
done
