
if [ "$#" -ne 0 ]; then
    echo "Usage: sh scripts/parse-all-references.sh"
    exit
fi

log_dir="logs/references/documents"
mkdir -p ${log_dir}

# https://stackoverflow.com/questions/20368577/bash-wildcard-n-digits
shopt -s extglob
for html_file_path in $(find data/references/html/html-+([0-9]).jsonl.bz2); do
  filename=$(basename ${html_file_path})
  qsub -N "parse-documents-${filename}" -o "${log_dir}/${filename}.stdout" -e "${log_dir}/${filename}.stderr" \
    scripts/references/parse-references.sh ${html_file_path}
done
