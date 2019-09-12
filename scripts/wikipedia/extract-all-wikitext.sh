
if [ "$#" -ne 0 ]; then
    echo "Usage: sh scripts/wikipedia/extract-all-wikitext.sh"
    exit
fi

num_shards=1000
log_dir="logs/wikipedia/wikitext"

mkdir -p ${log_dir}
for i in $(seq 0 $(expr ${num_shards} - 1)); do
  qsub -N "extract-wikitext-${i}" -o "${log_dir}/${i}.stdout" -e "${log_dir}/${i}.stderr" \
    scripts/wikipedia/extract-wikitext.sh ${i} ${num_shards}
done
