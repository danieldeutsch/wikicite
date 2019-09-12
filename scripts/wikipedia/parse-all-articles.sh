
if [ "$#" -ne 0 ]; then
    echo "Usage: sh scripts/wikipedia/parse-all-articles.sh"
    exit
fi

num_shards=1000
log_dir="logs/wikipedia/articles"

mkdir -p ${log_dir}
for i in $(seq 0 $(expr ${num_shards} - 1)); do
  qsub -N "parse-articles-${i}" -o "${log_dir}/${i}.stdout" -e "${log_dir}/${i}.stderr" \
    scripts/wikipedia/parse-articles.sh ${config} ${i}
done
