
if [ "$#" -ne 0 ]; then
    echo "Usage: sh scripts/references/extract-all-urls-to-crawl.sh"
    exit
fi

num_shards=1000
log_dir="logs/references/urls"

mkdir -p ${log_dir}
for i in $(seq 0 $(expr ${num_shards} - 1)); do
  qsub -N "extract-urls-${i}" -o "${log_dir}/${i}.stdout" -e "${log_dir}/${i}.stderr" \
    scripts/references/extract-urls-to-crawl.sh ${i}
done
