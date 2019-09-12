python -m wikicite.filters.quality_classifier \
  train \
  --input-jsonl data/mturk/gold.jsonl \
  --df-file data/filters/df.jsonl.gz \
  --views-file data/filters/views.jsonl.gz \
  --model-file data/filters/quality-model.pkl \
  --pr-curve-file pr.png \
  --pr-threshold-file pr.jsonl \
  --num-validation 100 \
  --error-file errors.jsonl
