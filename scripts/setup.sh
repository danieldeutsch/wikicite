ext_dir="ext"

# Setup Parsoid
parsoid_dir="${ext_dir}/parsoid"
parsoid_repo="https://github.com/wikimedia/parsoid"
parsoid_commit="1cefef127c51854952f8035ec4b17238a1eda96e"
if [ ! -d ${parsoid_dir} ]; then
  echo "Setting up Parsoid"
  mkdir -p $(dirname ${parsoid_dir})
  git clone ${parsoid_repo} ${parsoid_dir}
  git -C ${parsoid_dir} reset --hard ${parsoid_commit}
  npm install --prefix ${parsoid_dir}
else
  echo "Skipping setting up Parsoid"
fi

# Setup Unfluff
unfluff_dir="${ext_dir}/unfluff"
unfluff_repo="https://github.com/ageitgey/node-unfluff"
unfluff_commit="d9b67fbf90328d1a1bf75a6596491137a2587e33"
if [ ! -d ${unfluff_dir} ]; then
  echo "Setting up Unfluff"
  mkdir -p $(dirname ${unfluff_dir})
  git clone ${unfluff_repo} ${unfluff_dir}
  git -C ${unfluff_dir} reset --hard ${unfluff_commit}
  npm install --prefix ${unfluff_dir}
else
  echo "Skipping setting up Unfluff"
fi
