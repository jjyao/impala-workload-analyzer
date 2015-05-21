#!/bin/bash

# Usage: ./driver.sh tag input output

for file in $(find $2 -not -path '*/\.*' -type f); do
    cd python; python analyze_profiles.py $1 $file; cd ..
done

java -jar java/out/artifacts/query_analyzer/query_analyzer.jar $1

mkdir -p $3
python python/aggs.py $1 $3
python python/joins.py $1 $3
python python/stats.py $1 $3
