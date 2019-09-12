wiki_version="20190101"
data_dir="data/wikipedia"

# Download the Wikipedia xml
wikipedia_xml="${data_dir}/enwiki-${wiki_version}-pages-articles-multistream.xml.bz2"
wikipedia_dump_url="https://dumps.wikimedia.org/enwiki/${wiki_version}/enwiki-${wiki_version}-pages-articles-multistream.xml.bz2"
if [ ! -f ${wikipedia_xml} ]; then
  echo "Downloading Wikipedia xml"
  mkdir -p $(dirname ${wikipedia_xml})
  wget ${wikipedia_dump_url} -O ${wikipedia_xml}
else
  echo "Skipping downloading Wikipedia xml"
fi

# Download the Wikipedia xml index
wikipedia_xml_index="${data_dir}/enwiki-${wiki_version}-pages-articles-multistream.xml.bz2"
wikipedia_dump_index_url="https://dumps.wikimedia.org/enwiki/${wiki_version}/enwiki-${wiki_version}-pages-articles-multistream-index.txt.bz2"
if [ ! -f ${wikipedia_xml_index} ]; then
  echo "Downloading Wikipedia xml index"
  mkdir -p $(dirname ${wikipedia_xml_index})
  wget ${wikipedia_dump_index_url} -O ${wikipedia_xml_index}
else
  echo "Skipping downloading Wikipedia xml index"
fi
