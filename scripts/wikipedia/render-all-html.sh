
if [ "$#" -ne 0 ]; then
    echo "Usage: sh scripts/wikipedia/render-all-html.sh"
    exit
fi

num_shards=1000
log_dir="logs/wikipedia/html"

mkdir -p ${log_dir}
for i in $(seq 0 $(expr ${num_shards} - 1)); do
  qsub -N "render-html-${i}" -o "${log_dir}/${i}.stdout" -e "${log_dir}/${i}.stderr" \
    scripts/wikipedia/render-html.sh ${i}
done
