#!/bin/bash
set -e

for i in {1..30}; do
  if hdfs dfs -test -d / 2>/dev/null; then
    break
  fi
  sleep 5
done

hdfs dfs -mkdir -p /data
hdfs dfs -put -f /data/local/wiki_reference.csv /data/wiki_reference.csv
